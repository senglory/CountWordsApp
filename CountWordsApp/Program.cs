using System;
using System.Diagnostics;
using System.IO;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using CommandLine;
using CommandLine.Text;

namespace CountWordsApp
{
    class Options
    {
        [Option('l', "length", Required = true,
          HelpText = "Word's length threshold.")]
        public int WordLengthThreshold { get; set; }

        [Option('q', "queue", DefaultValue = 1000,
          HelpText = "Queue size (in lines) for reading files.")]
        public int QueueSize { get; set; }

        [Option('b', "buffer", DefaultValue = 1000,
          HelpText = "Buffer size (in lines) for calculating statistics.")]
        public int BufferSize { get; set; }

        [Option('p', "path", DefaultValue = ".",
          HelpText = "Path to directory. Must be enclosed with double quotas.")]
        public string Path { get; set; }

        [Option('i', "ignore", DefaultValue = false,
          HelpText = "Ignore case while searching words")]
        public bool IgnoreCase { get; set; }

        [Option('v', "verbose", DefaultValue = false,
          HelpText = "Verbose output.")]
        public bool Verbose { get; set; }
        [HelpOption]
        public string GetUsage()
        {
            return HelpText.AutoBuild(this,
              (HelpText current) => HelpText.DefaultParsingErrorsHandler(this, current));
        }
    }

    class StringAndFile
    {
        public string Str;
        public string File;
        public int Line;
    }

    class Program
    {
        public readonly static object _forLockingInp = new object();
        public readonly static object _forLockingStats = new object();
        public readonly static Options options = new Options();
        public static string[] allFiles;

        public static List<Tuple<string, int>> GetTopWords(int size, Dictionary<string, int> allWordsInAllFiles)
        {
            var res = new List<Tuple<string, int>>(size);
            var tmpls = allWordsInAllFiles.ToList();
            //var comparer = Comparer<KeyValuePair<string, int>>.Create((x, y) => x.Value.CompareTo(y.Value));
            // for .NET >= 4.5 
            var comparer = Comparer<KeyValuePair<string, int>>.Create((x, y) => y.Value.CompareTo(x.Value));
            //var comparer = new Comparer<KeyValuePair<string, int>>();// .Create((x, y) => y.Value.CompareTo(x.Value));


            tmpls.Sort(comparer);
            var tt = tmpls.Take(size);
            foreach (var v in tt)
            {
                res.Add(new Tuple<string, int>(v.Key, v.Value));
            }
            return res;

            var highestIndices = new List<KeyValuePair<string, int>>(size);
            foreach (var v in tmpls)
            {
                if (highestIndices.Count < size)
                    highestIndices.Add(v);
                else if (comparer.Compare(highestIndices[0], v) < 0)
                {
                    highestIndices.Remove(highestIndices[0]);
                    highestIndices.Add(v);
                }
                highestIndices.Sort(comparer);
            }
            foreach (var v in highestIndices)
            {
                res.Add(new Tuple<string, int>(v.Key, v.Value));
            }
            return res;
        }
        readonly static EventWaitHandle waiterForStats = new ManualResetEvent(false);
        readonly static EventWaitHandle waiterForAgg = new AutoResetEvent(false);
        readonly static EventWaitHandle finishedWorking = new ManualResetEvent(false);
        //readonly static EventWaitHandle finishedWorkingStats = new ManualResetEvent(false);
        static Semaphore semForStats;


        static Queue<StringAndFile> _queueForStrings ;
        static readonly Queue<Dictionary<string, int>> _queueForStats=new Queue<Dictionary<string, int>>();
        static Dictionary<string, int> _allWordsInAllFiles;

        readonly static Regex rxsepar = new Regex( @"[^\s\t]+", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.IgnorePatternWhitespace );

        static int _lines_GLOBAL_counter = 0;
        static void ReadFiles()
        {
            foreach (var fn in allFiles)
            {
                try
                {
                    using (var fileStream = File.Open(fn, FileMode.Open, FileAccess.Read))
                    {
                        if (options.Verbose)
                            Console.WriteLine("Processing " + fn);
                        var fnDEBUG = Path.GetFileName(fn);
int linCnt = 1;
                        using (var streamReader = new StreamReader(fileStream))
                        {
                            do
                            {
                                string line;
                                lock (_forLockingInp)
                                {
                                    if (_queueForStrings.Count == options.QueueSize)
                                        continue;
                                    while ((line = streamReader.ReadLine()) != null)
                                    {
                                        if (string.IsNullOrWhiteSpace(line))
                                            continue;
                                        var o = new StringAndFile() { Str = line, File = fnDEBUG, Line = linCnt };
linCnt++;
_lines_GLOBAL_counter++;
Debug.WriteLine("Line: " + o.Line + " File " + o.File);
                                        _queueForStrings.Enqueue(o);
                                        if (_queueForStrings.Count == options.QueueSize)
                                            break;
                                    }
                                }
                                #region Pass the queue to the calc threads
                                waiterForStats.Set();
                                #endregion
                            }
                            while (!streamReader.EndOfStream);
                        }
                    }
                }
                catch (IOException)
                {
                    // skip failed attempts
                }
            }
            if (options.Verbose)
                Console.WriteLine("IO ops - done");
            finishedWorking.Set();
            _done=true;
        }


        static volatile bool _done = false;

        static void CalcStats()
        {
int local_cnt = 0;
            while (true)
            {
                int who = WaitHandle.WaitAny(new WaitHandle[] { waiterForStats, finishedWorking });
                //waiterForStats.WaitOne();
                List<string> tmp = new List<string>(options.BufferSize);
                Dictionary<string, int> wordCount = options.IgnoreCase ?
                    new Dictionary<string, int>(options.BufferSize, StringComparer.InvariantCultureIgnoreCase)
                    : new Dictionary<string, int>(options.BufferSize, StringComparer.InvariantCulture);
                lock (_forLockingInp)
                {
                    if (_queueForStrings.Count == 0 && 1 == who)
                        break;
                    if (_queueForStrings.Count == 0 && _done)
                        break;
                    for (int j = 0; j < options.BufferSize && _queueForStrings.Count > 0; j++)
                    {
                        var s = _queueForStrings.Dequeue();
                        tmp.Add(s.Str);
Debug.WriteLine(Thread.CurrentThread.Name + "\t" +s.Line + "\t" + s.File);
local_cnt++;
                    }
                }
                #region Calculate local stats
                foreach (var str in tmp)
                {
                    #region Split string into words
                    var matches = rxsepar.Matches(str);
                    var words = new List<string>();
                    foreach (Match m in matches)
                    {
                        words.Add(m.Value);
                    }
                    #endregion

                    foreach (var word in words)
                    {
                        if (word.Length < options.WordLengthThreshold)
                            continue;
                        if (wordCount.ContainsKey(word))
                        {
                            wordCount[word] = wordCount[word] + 1;
                        }
                        else
                        {
                            wordCount.Add(word, 1);
                        }
                    }
                }
                #endregion
                lock (_forLockingStats)
                {
                    _queueForStats.Enqueue(wordCount);
                }
                waiterForAgg.Set();
            }
var sss = string.Format(Thread.CurrentThread.Name + " local lines: {0}", local_cnt);
Debug.WriteLine(sss);
Console.WriteLine(sss);
        }

        static void CalcAgg()
        {
int local_cnt = 0;
            while (true)
            {
                int who = WaitHandle.WaitAny(new WaitHandle[] { waiterForAgg, finishedWorking });

                Dictionary<string, int> wordCountDict = null;
                lock (_forLockingStats)
                {
                    if (_queueForStats.Count > 0)
                        wordCountDict = _queueForStats.Dequeue();
                    else
                    if (1 == who)
                    {
                        if (options.Verbose)
                            Console.WriteLine("Aggregation - DONE");
Console.WriteLine(string.Format("AGG local # of sets: {0}", local_cnt));
Debug.WriteLine(string.Format("AGG local # of sets: {0}", local_cnt));
                        //waiterForAgg.Reset();
                        return;
                    }
                }
                if (wordCountDict != null)
                {
Debug.WriteLine(string.Format("AGG # wordCountDict: {0}", wordCountDict.Keys.Count));
local_cnt++;
                    foreach (var kv in wordCountDict.Keys)
                    {
                        int cnt2 = wordCountDict[kv];
                        if (_allWordsInAllFiles.ContainsKey(kv))
                            cnt2 += _allWordsInAllFiles[kv];
                        _allWordsInAllFiles[kv] = cnt2;
                    }
                }
            }
        }




        static void Main(string[] args)
        {
            if (CommandLine.Parser.Default.ParseArguments(args, options))
            {
                int cpus = 2;// Math.Min(Environment.ProcessorCount - 2, 1);
                List<Thread> calcThreads = new List<Thread>(cpus);
                semForStats = new Semaphore(cpus, cpus);
                Thread threadFOrIO;
                Thread threadFOrAggregation;

                if (options.IgnoreCase)
                    _allWordsInAllFiles = new Dictionary<string, int>(StringComparer.InvariantCultureIgnoreCase);
                else
                    _allWordsInAllFiles = new Dictionary<string, int>(StringComparer.InvariantCulture);

                // quick workaround for bug in CommandLine
                var p = options.Path;
                if (options.Path.EndsWith("\""))
                    p = p.Substring(0, p.Length - 1) + "\\";
                try
                {
                    allFiles = Directory.GetFiles(p, "*.txt", SearchOption.TopDirectoryOnly);

                    if (options.Verbose)
                        Console.WriteLine("Number of files: {0}", allFiles.Length);
                    if (options.Verbose)
                    {
                        Console.WriteLine("Number of CPUs: {0}", cpus);
                        Console.WriteLine("Statistics will be prepared by {0} threads", cpus);
                    }

                    _queueForStrings = new Queue<StringAndFile>(options.QueueSize);
                    threadFOrIO = new Thread(ReadFiles);

                    for (int i = 0; i < cpus; i++)
                    {
                        var t = new Thread( CalcStats)
                        { Name = "CalcTread " + i.ToString() };
                        calcThreads.Add(t);
                    }

                    threadFOrAggregation = new Thread(CalcAgg) { Name = "AGG"};

                    threadFOrIO.Start();
                    calcThreads.ForEach((t) =>
                       {
                           t.Start();
                       });
                    threadFOrAggregation.Start();

                    //Console.WriteLine("Press ENTER to view results & exit...");
                    //Console.ReadLine();
                    //finishedWorking.Set();
                    threadFOrIO.Join();

                    //finishedWorking.Set();
                    //waiterForStats.Set();

                    calcThreads.ForEach((t) => t.Join());
                    threadFOrAggregation.Join();


                    var r = GetTopWords(10, _allWordsInAllFiles);
                    Console.WriteLine("========= RESULTS =========");
                    r.ForEach((v) => Console.WriteLine(v.Item1 + " " + v.Item2));
                    r.ForEach((v) => Debug.WriteLine(v.Item1 + " " + v.Item2));
Console.WriteLine(string.Format ("LINES: {0} {1}" , _lines_GLOBAL_counter, _queueForStrings.Count));
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                    return;
                }
            }
        }
    }
}
