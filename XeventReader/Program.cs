using CsvHelper;
using Microsoft.SqlServer.XEvent.XELite;
using System.Dynamic;
using System.Globalization;

var server = "localhost";
var sessionName = "MySession";
var fields = new List<string>() { "cpu_time", "duration", "writes", "batch_text", "database_name" };
var fileName = "c:\\temp\\xevent.csv";


var cancellationTokenSource = new CancellationTokenSource();

Console.WriteLine("Press any key to stop listening...");
Task waitTask = Task.Run(() =>
{
    Console.ReadKey();
    cancellationTokenSource.Cancel();
});


using var writer = new StreamWriter(fileName);
using var csv = new CsvWriter(writer, CultureInfo.InvariantCulture);

var d = new ExpandoObject();
d.TryAdd("server", null); 
d.TryAdd("session_name", null);
d.TryAdd("event_name", null);
d.TryAdd("timestamp", null);
foreach (var field in fields)
{
    d.TryAdd(field, null);
}
csv.WriteDynamicHeader(d);
csv.NextRecord();

var readTask = Read(server,sessionName, cancellationTokenSource.Token, (server,session,evt) => OnEventReceived(server, session, evt, csv, fields) );

waitTask.Wait();

Console.WriteLine("shutting down...");
readTask.Wait();

static void OnEventReceived(string server, string sessionName, IXEvent xEvent, CsvWriter csv, IList<string> fields)
{
    lock (csv)
    {
        Console.WriteLine("Event Received");

        csv.WriteField(server, true);
        csv.WriteField(sessionName, true);
        csv.WriteField(xEvent.Name, true);
        csv.WriteField(xEvent.Timestamp.ToString("o"), true);
        foreach(var field in fields)
        {
            if (xEvent.Fields.ContainsKey(field))
            {
                csv.WriteField(xEvent.Fields[field].ToString(), true);
            }
            else if (xEvent.Actions.ContainsKey(field))
            {
                csv.WriteField(xEvent.Actions[field].ToString(), true);
            }
            else
            {
                csv.WriteField("", true);
            }
        }
        csv.NextRecord();
    }

}


static async Task Read(string server, string sessionName, CancellationToken cancel, Action<string, string, IXEvent> onEventReceived )
{
    var xeStream = new XELiveEventStreamer($"server={server};Integrated Security=true;TrustServerCertificate=true", sessionName);
    
    Task readTask = xeStream.ReadEventStream(() =>
    {
        Console.WriteLine("Connected to session");
        return Task.CompletedTask;
    },
        xevent =>
        {
            onEventReceived(server,sessionName, xevent);
            //Console.WriteLine("");
            return Task.CompletedTask;
        },
        cancel);



}