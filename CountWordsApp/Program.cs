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


    class Program
    {
        public readonly static object _forLockingInp = new object();
        public readonly static object _forLockingStats = new object();

        public static List<Tuple<string, int>> GetTopWords(int size, Dictionary<string, int> allWordsInAllFiles)
        {
            var res = new List<Tuple<string, int>>(size);
            var comparer = Comparer<KeyValuePair<string, int>>.Create((x, y) => x.Value.CompareTo(y.Value));
            var highestIndices = new List<KeyValuePair<string, int>>(size);
            var tmpls = allWordsInAllFiles.ToList();
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
                res.Add( new Tuple<string, int>(v.Key, v.Value ));
            }
            return res;
        }
        readonly static EventWaitHandle waiterForStats = new AutoResetEvent(false);
        readonly static EventWaitHandle waiterForAgg = new AutoResetEvent(false);

        static  Queue<string> queueForStrings ;
        static readonly Queue<Dictionary<string, int>> queueForStats=new Queue<Dictionary<string, int>>();
        static Dictionary<string, int> _allWordsInAllFiles;

        readonly static Regex rxsepar = new Regex( @"[^\s\t]+", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.IgnorePatternWhitespace );

        static void Main(string[] args)
        {
            var options = new Options();
            if (CommandLine.Parser.Default.ParseArguments(args, options))
            {
                int cpus = 2;// Math.Min(Environment.ProcessorCount - 2, 1);
                List<Thread> calcThreads = new List<Thread>(cpus);
                Thread threadFOrIO;
                Thread threadFOrAggregation;
                EventWaitHandle weAreDone = new ManualResetEvent(false);
                CancellationTokenSource canstoken = null;// new CancellationTokenSource();

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
                    var allFiles = Directory.GetFiles(p, "*.txt", SearchOption.TopDirectoryOnly);

                    if (options.Verbose)
                        Console.WriteLine("Number of files: {0}", allFiles.Length);
                    if (options.Verbose)
                    {
                        Console.WriteLine("Number of CPUs: {0}", cpus);
                        Console.WriteLine("Statistics will be prepared by {0} threads", cpus);
                    }

                    for (int i = 0; i < cpus; i++)
                    {
                        var t = new Thread((cts) =>
                        {
                            while (true)
                            {
                                int who = WaitHandle.WaitAny(new WaitHandle[] { waiterForStats, weAreDone });
                                if (1 == who)
                                    return;

                                List<string> tmp = new List<string>(options.BufferSize);
                                Dictionary<string, int> wordCount = options.IgnoreCase ?
                                    new Dictionary<string, int>(options.BufferSize, StringComparer.InvariantCultureIgnoreCase)
                                    : new Dictionary<string, int>(options.BufferSize, StringComparer.InvariantCulture);
                                lock (_forLockingInp)
                                {
                                    for (int j = 0; j < options.BufferSize && queueForStrings.Count > 0; j++)
                                    {
                                        var s = queueForStrings.Dequeue();
                                        tmp.Add(s);
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
                                    queueForStats.Enqueue(wordCount);
                                }
                                waiterForAgg.Set();
                            }
                        });
                        calcThreads.Add(t);
                    }

                    queueForStrings = new Queue<string>(options.QueueSize);
                    threadFOrIO = new Thread((cts) => {
                        foreach (var fn in allFiles)
                        {
                            try
                            {
                                using (var fileStream = File.Open(fn, FileMode.Open, FileAccess.Read))
                                using (var streamReader = new StreamReader(fileStream))
                                {
                                    do
                                    {
                                        string line;
                                        while ((line = streamReader.ReadLine()) != null && queueForStrings.Count < options.QueueSize)
                                        {
                                            lock (_forLockingInp)
                                                queueForStrings.Enqueue(line);
                                        }
                                        #region Pass the queue to the calc threads
                                        waiterForStats.Set();
                                        #endregion
                                    }
                                    while (!streamReader.EndOfStream);
                                }
                            }
                            catch (IOException)
                            {
                                // skip failed attempts
                            }
                            break;
                        }
                        weAreDone.Set();
                        waiterForStats.Set();
                        waiterForAgg.Set();
                    }) ;

                    threadFOrAggregation = new Thread((cts) => {
                        while (true)
                        {
                            int who = WaitHandle.WaitAny(new WaitHandle[] { waiterForAgg, weAreDone });
                            //if (1 == who)
                            //{
                            //    Console.WriteLine("DONE");
                            //    return;
                            //}
                            Dictionary<string, int> wordCountDict = null;
                            lock (_forLockingStats)
                            {
                                if (queueForStats.Count > 0)
                                    wordCountDict = queueForStats.Dequeue();
                            }
                            if (wordCountDict != null) { 
                                foreach (var kv in wordCountDict.Keys)
                                {
                                    int cnt = wordCountDict[kv];
                                    if (_allWordsInAllFiles.ContainsKey(kv))
                                        cnt += _allWordsInAllFiles[kv] ;
                                    _allWordsInAllFiles[kv] = cnt;
                                }
                            }
                            if (1 == who)
                            {
                                Console.WriteLine("DONE");
                                return;
                            }
                        }
                    });

                    calcThreads.ForEach((t) =>
                       {
                           t.Start(canstoken);
                       });
                    threadFOrIO.Start(canstoken);
                    threadFOrAggregation.Start(canstoken);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                    return;
                }
                //weAreDone.WaitOne();
                Console.WriteLine("Press ENTER to view results & exit...");
                Console.ReadLine();


                var r = GetTopWords(10, _allWordsInAllFiles);
                Console.WriteLine("========= RESULTS =========");
                r.ForEach((v) => Console.WriteLine(v.Item1 + " " + v.Item2));
            }
        }
    }
}
