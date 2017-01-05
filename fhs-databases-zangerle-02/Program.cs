using Neo4j.Driver.V1;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace fhs_databases_zangerle_02
{
    class Program
    {

        public static readonly string ExePath = System.Reflection.Assembly.GetExecutingAssembly().Location;
        public static readonly string ExeDir = Path.GetDirectoryName(ExePath);

        public static readonly string DataMiniDirName = "meetup-data-mini";
        public static readonly string DataExtractDirName = "meetup-data-extract";
        public static readonly string DataFullDirName = "meetup-data-full";

        static void Main(string[] args)
        {
            SeedDatabase();

            Console.ReadLine();
        }


        public static void SeedDatabase()
        {
            //SeedGroups();
            //SeedMembers();
        }


        public static string ReadFile(string filename)
        {
            using (StreamReader r = new StreamReader(filename))
            {
                return r.ReadToEnd();
            }
        }


        public static void SeedGroups()
        {
            using (var driver = GraphDatabase.Driver("bolt://localhost", AuthTokens.Basic("neo4j", "pass")))
            using (var session = driver.Session())
            {
                var files = Directory.GetFiles(Path.Combine(DataMiniDirName, "groups"));
                foreach (var file in files)
                {
                    Console.WriteLine($"Start parsing file: {file}");
                    var jsonRaw = ReadFile(file);
                    jsonRaw = jsonRaw.Replace("'", "");
                    dynamic array = JsonConvert.DeserializeObject(jsonRaw);

                    if (array.Count == 0)
                        continue;

                    foreach (var group in array)
                    {
                        session.Run($"CREATE (n:Group{{id:'{group.id}', name:'{group.name}'}})");
                    }
                }
                Console.WriteLine("Finished seeding groups");
            }
        }

        public static void SeedMembers()
        {
            using (var driver = GraphDatabase.Driver("bolt://localhost", AuthTokens.Basic("neo4j", "pass")))
            using (var session = driver.Session())
            {
                var files = Directory.GetFiles(Path.Combine(DataMiniDirName, "members"));
                foreach (var file in files)
                {
                    Console.WriteLine($"Start parsing file: {file}");
                    var jsonRaw = ReadFile(file);
                    jsonRaw = jsonRaw.Replace("'", "");
                    dynamic array = JsonConvert.DeserializeObject(jsonRaw);

                    if (array.Count == 0)
                        continue;

                    foreach (var member in array)
                    {
                        foreach (var topic in member.topics)
                        {
                            var stringToExecute = $"MERGE (member:Member{{id:'{member.id}',name:'{member.name}'}}) MERGE (topic:Topic{{id:'{topic.id}',name:'{topic.name}'}}) MERGE (member)-[:IsInterestedIn]->(topic)";
                            Console.WriteLine(stringToExecute);
                            session.Run(stringToExecute);
                        }
                    }
                }
                Console.WriteLine("Finished seeding members");
            }
        }


        public static void RsvpFolderFlatter()
        {
            var dataSetPath = Path.Combine(ExeDir, DataExtractDirName);
            var rsvpFolderPath = Path.Combine(dataSetPath, "rsvps");

            var folders = Directory.GetDirectories(rsvpFolderPath);

            var index = 1;
            foreach (var folder in folders)
            {
                Console.WriteLine($"{index}/{folders.Length}: Flat folder: {folder}");
                var files = Directory.GetFiles(folder);
                foreach (var file in files)
                {
                    var destinationPath = Path.Combine(rsvpFolderPath, Path.GetFileName(file));
                    File.Move(file, destinationPath);
                }
                Directory.Delete(folder);
                index++;
            }
            Console.WriteLine("Flattening finished.");
        }
    }
}
