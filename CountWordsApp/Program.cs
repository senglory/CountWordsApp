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
        public readonly static Options options = new Options();

        public readonly static object _forLockingInp = new object();
        public readonly static object _forLockingStats = new object();
        public static string[] allFiles;

        public static List<Tuple<string, int>> GetTopWords(int size, Dictionary<string, int> allWordsInAllFiles)
        {
            var res = new List<Tuple<string, int>>(size);
            var tmpls = allWordsInAllFiles.ToList();
            var comparer = Comparer<KeyValuePair<string, int>>.Create((x, y) => x.Value.CompareTo(y.Value));

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
        readonly static EventWaitHandle waiterForAgg = new ManualResetEvent(false);
        readonly static EventWaitHandle _finishedWorking = new ManualResetEvent(false);
        static CountdownEvent _countdownForStats;


        static Queue<string> _queueForStrings ;
        static readonly Queue<Dictionary<string, int>> _queueForStats=new Queue<Dictionary<string, int>>();
        static Dictionary<string, int> _allWordsInAllFiles = new Dictionary<string, int>(StringComparer.InvariantCultureIgnoreCase);

        readonly static Regex rxsepar = new Regex( @"[^\s\t]+", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.IgnorePatternWhitespace );

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
                        using (var streamReader = new StreamReader(fileStream))
                        {
                            do
                            {
                                lock (_forLockingInp)
                                {
                                    // buffer is full - run statistics calculation
                                    if (_queueForStrings.Count == options.QueueSize)
                                    {
                                        waiterForStats.Set();
                                        continue;
                                    }
                                    string line;
                                    // populate buffer until there's a room 
                                    while ((line = streamReader.ReadLine()) != null)
                                    {
                                        // skip empty lines
                                        if (string.IsNullOrWhiteSpace(line))
                                            continue;
                                        _queueForStrings.Enqueue(line);
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
            _finishedWorking.Set();
        }


        static void CalcStats()
        {
            while (true)
            {
                int who = WaitHandle.WaitAny(new WaitHandle[] { waiterForStats, _finishedWorking });
                List<string> tmp = new List<string>(options.BufferSize);
                Dictionary<string, int> wordCount = new Dictionary<string, int>(options.BufferSize, StringComparer.InvariantCultureIgnoreCase);
                lock (_forLockingInp)
                {
                    // no more data and we're requested to finish - quit
                    if (_queueForStrings.Count == 0 && 1 == who)
                        break;
                    for (int j = 0; j < options.BufferSize && _queueForStrings.Count > 0; j++)
                    {
                        var s = _queueForStrings.Dequeue();
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

                if (wordCount.Keys.Count > 0)
                {
                    lock (_forLockingStats)
                    {
                        _queueForStats.Enqueue(wordCount);
                    }
                }
                // pulse aggregation
                waiterForStats.Reset();
                waiterForAgg.Set();
            }
            _countdownForStats.Signal();
        }

        static void CalcAgg()
        {
            while (true)
            {
                int who = WaitHandle.WaitAny(new WaitHandle[] { waiterForAgg, _countdownForStats.WaitHandle });
                // pulse aggregation
                if (0 == who )
                    waiterForAgg.Reset();
                Dictionary<string, int> wordCountDict = null;
                lock (_forLockingStats)
                {
                    if (_queueForStats.Count > 0)
                        wordCountDict = _queueForStats.Dequeue();
                    else
                    if (_queueForStats.Count == 0 && 1 == who)
                    {
                        // shutdown statistics calculators
                        waiterForStats.Reset();

                        if (options.Verbose)
                            Console.WriteLine("Aggregation - DONE");
                        break;
                    }
                }
                if (wordCountDict != null)
                {
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
            if (!CommandLine.Parser.Default.ParseArguments(args, options))
            {
                return;
            }
            int cpus = Math.Min(Environment.ProcessorCount - 2, 1);
            List<Thread> calcThreads = new List<Thread>(cpus);
            _countdownForStats = new CountdownEvent(cpus);
            Thread threadFOrIO;
            Thread threadFOrAggregation;


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

                _queueForStrings = new Queue<string>(options.QueueSize);
                threadFOrIO = new Thread(ReadFiles);

                for (int i = 0; i < cpus; i++)
                {
                    var t = new Thread( CalcStats);
                    calcThreads.Add(t);
                }

                threadFOrAggregation = new Thread(CalcAgg);

                threadFOrIO.Start();
                calcThreads.ForEach((t) =>
                    {
                        t.Start();
                    });
                threadFOrAggregation.Start();

                // waiting for finishing
                threadFOrIO.Join();
                calcThreads.ForEach((t) => t.Join());
                threadFOrAggregation.Join();

                var r = GetTopWords(10, _allWordsInAllFiles);
                Console.WriteLine("========= RESULTS =========");
                r.ForEach((v) => Console.WriteLine(v.Item1 + " " + v.Item2));
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                return;
            }
        }
    }
}
