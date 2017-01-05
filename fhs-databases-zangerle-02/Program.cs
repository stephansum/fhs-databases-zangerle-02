using Neo4j.Driver.V1;
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
            //SeedDatabase();
            Test(Path.Combine(DataExtractDirName, "members"));

            Console.ReadLine();
        }


        public static void SeedDatabase()
        {
            SeedMembers();
        }

        public static void Test(string path)
        {
            var files = Directory.GetFiles(path);
            foreach (var file in files)
            {
                Console.WriteLine($"Start parsing file: {file}");
                var jsonRaw = ReadFile(file);
                jsonRaw = jsonRaw.Replace("'", "''");
                Console.WriteLine(jsonRaw);
            }
            
        }


        public static string ReadFile(string filename)
        {
            using (StreamReader r = new StreamReader(filename))
            {
                return r.ReadToEnd();
            }
        }


        public static void SeedMembers()
        {
            using (var driver = GraphDatabase.Driver("bolt://localhost", AuthTokens.Basic("neo4j", "pass")))
            using (var session = driver.Session())
            {
                session.Run("CREATE (a:Person {name:'Arthur', title:'King'})");
                var result = session.Run("MATCH (a:Person) WHERE a.name = 'Arthur' RETURN a.name AS name, a.title AS title");

                foreach (var record in result)
                {
                    Console.WriteLine($"{record["title"].As<string>()} {record["name"].As<string>()}");
                }
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
