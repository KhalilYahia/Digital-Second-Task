
using System.Globalization;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Net;

using System.Text;

record GeoInfo(string CountryCode, string CountryName, string StateCode, string StateName);

class Node
{
    public int ValueIndex = -1;
    public Node[]? Children;
    public long StartGeoInfo_Address = -1;
    public long EndGeoInfo_Address = -1;
    public GeoInfo info=null;

    public int test = 0;
}


public class NodeFile
{
    public int ValueIndex;
    public int ChildrenCount;
    public long Children_Address;
    public long StartGeoInfo_Address;
    public long EndGeoInfo_Address;

    public static NodeFile Create()
    {
        return new NodeFile
        {
            ValueIndex = -1,
            Children_Address = 0,
            StartGeoInfo_Address = -1,
            EndGeoInfo_Address = -1
        };
    }
}
struct BinaryFileHeader
{ 
    public long GeoInfoOffset;
    public long NodesOffset;
    
    public BinaryFileHeader() { }
}
class BuildOfBinaryFile
{
  
    public void Build(string csvPath, string outPathIpv4, string outPathIpv6)
    {
        if (!File.Exists(csvPath)) { Console.Error.WriteLine("CSV file not found."); return; }

        var rootv4 = new Node();
        var rootv6 = new Node();

        var valueMap = new Dictionary<(string cc, string cn, string sc, string sn), int>();
        var values = new List<(string cc, string cn, string sc, string sn)>();

        long lineNo = 0;
        foreach (var line in File.ReadLines(csvPath, Encoding.UTF8))
        {
            lineNo++;
            if (string.IsNullOrWhiteSpace(line)) continue;
            var p = SplitCsvFast(line, 8);
            if (p.Count < 7) continue;

            string cidr = p[0];

          
           var isV4= TryParseCidrV4(cidr, out byte[] ipv4, out int prefixv4);
           TryParseCidrV6(cidr, out byte[] ipv6, out int prefixV6);

            if (isV4)
            {
                InsertPrefix(rootv4, ipv4, prefixv4, new GeoInfo(p[3], p[4], p[5], p[6]), lineNo);
            }
            else
            {
                InsertPrefix(rootv6, ipv6, prefixV6, new GeoInfo(p[3], p[4], p[5], p[6]), lineNo);
            }

            if ((lineNo & ((1 << 18) - 1)) == 0) Console.Error.WriteLine($"Read ~{lineNo:n0} lines...");
        }

        // Create Ipv4 File
        BuildIPFile(rootv4, outPathIpv4);

        // Create Ipv6 File
        BuildIPFile(rootv6, outPathIpv6);

             
    }

    private void BuildIPFile(Node root, string outPath)
    {
       
        using var fs = File.Create(outPath);
        using var bw = new BinaryWriter(fs, Encoding.UTF8, leaveOpen: true);

        BinaryFileHeader header = new BinaryFileHeader();

        long HeaderStartPos = fs.Position;
        // write to header file        
        for (int i = 0; i < 28; ++i) bw.Write(new byte());

        long GeoInfoStartPos = fs.Position;
        Write_GeoInfoToFile(root.Children, fs, bw);
        long NodesStartPos = fs.Position;
        Write_NodesToFile(new Node[] { root }, fs, bw);

        // 
        fs.Position = HeaderStartPos;
        bw.Write(GeoInfoStartPos);
        bw.Write(NodesStartPos);
        bw.Write(0);

       
    }

    List<string> SplitCsvFast(string line, int takeFirstN)
    {
        var res = new List<string>(takeFirstN);
        int start = 0;
        for (int i = 0; i < line.Length && res.Count < takeFirstN - 1; i++)
            if (line[i] == ',') { res.Add(line.Substring(start, i - start)); start = i + 1; }
        res.Add(start <= line.Length ? line[start..] : "");
        return res;
    }

    bool TryParseCidrV4(string cidr, out byte[] ip, out int prefix)
    {
        ip = new byte[4]; prefix = 0;
        int slash = cidr.IndexOf('/');
        if (slash <= 0) return false;
        string ipStr = cidr[..slash];
        if (!int.TryParse(cidr[(slash + 1)..], NumberStyles.Integer, CultureInfo.InvariantCulture, out prefix))
            return false;
        if (prefix < 0 || prefix > 32) return false;
        if (!IPAddress.TryParse(ipStr, out var ipa)) return false;
        if (ipa.AddressFamily != System.Net.Sockets.AddressFamily.InterNetwork) return false;
        ip = ipa.GetAddressBytes();
        return true;
    }

    bool TryParseCidrV6(string cidr, out byte[] ipV6, out int prefixV6)
    {
        ipV6 = new byte[16]; prefixV6 = 0;
        int slash = cidr.IndexOf('/');
        if (slash <= 0) return false;
        string ipStr = cidr[..slash];
        if (!int.TryParse(cidr[(slash + 1)..], NumberStyles.Integer, CultureInfo.InvariantCulture, out prefixV6))
            return false;
        if (prefixV6 < 0 || prefixV6 > 128) return false;
        if (!System.Net.IPAddress.TryParse(ipStr, out var ipa)) return false;
        if (ipa.AddressFamily != System.Net.Sockets.AddressFamily.InterNetworkV6) return false;
        ipV6 = ipa.GetAddressBytes();
        return true;
    }
   

    void InsertPrefix(Node root, byte[] ipByte, int prefixLen, GeoInfo info,long lineNo)
    {
       
        int depthBytes = (int) Math.Ceiling(((float)prefixLen)/8.0);  

        var node = root;
        for (int i = 0; i < depthBytes; i++)
        {
            var k = ipByte[i];
            node.Children ??= new Node[256];
            node.Children[k] ??= new Node();
            node.Children[k].ValueIndex = k;
           
            node = node.Children[k]!;
        }

        node.info = info;
       // node.GeoInfo_Line = lineNo;

    }

    

    Dictionary<int, long> Write_NodesToFile( Node[] nodes, FileStream fs, BinaryWriter bw)
    {
        if (nodes == null || nodes.Length == 0) return new Dictionary<int, long>();

        // First, do a complete BFS to build the tree structure and calculate positions
        List<List<Node>> levels = new List<List<Node>>();
        Dictionary<Node, (int level, int index)> nodePositions = new Dictionary<Node, (int, int)>();
        Dictionary<Node, long> nodeAddresses = new Dictionary<Node, long>();

        Queue<(Node node, int level)> queue = new Queue<(Node node, int level)>();
        foreach (var node in nodes)
        {
            if (node != null)
            {
                queue.Enqueue((node, 0));
            }
        }

        // Build level structure
        while (queue.Count > 0)
        {
            var (currentNode, level) = queue.Dequeue();

            if (level >= levels.Count)
                levels.Add(new List<Node>());

            int indexInLevel = levels[level].Count;
            levels[level].Add(currentNode);
            nodePositions[currentNode] = (level, indexInLevel);

            if (currentNode.Children != null)
            {
                foreach (var child in currentNode.Children)
                {
                    if (child != null)
                    {
                        queue.Enqueue((child, level + 1));
                    }
                }
            }
        }

        // Calculate addresses for all levels
        Dictionary<int, long> levelAddresses = new Dictionary<int, long>();
        long currentAddress = fs.Position;
        int nodeSize = 32;// System.Runtime.InteropServices.Marshal.SizeOf(typeof(NodeFile));

        for (int level = 0; level < levels.Count; level++)
        {
            levelAddresses[level] = currentAddress;
            currentAddress += levels[level].Count * nodeSize;
        }

        // Calculate addresses for individual nodes and their children
        foreach (var level in levels)
        {
            foreach (var node in level)
            {
                var (nodeLevel, nodeIndex) = nodePositions[node];
                nodeAddresses[node] = levelAddresses[nodeLevel] + (nodeIndex * nodeSize);
            }
        }

        // Write all nodes with correct children addresses
        for (int level = 0; level < levels.Count; level++)
        {
            foreach (var node in levels[level])
            {
                NodeFile nodeFile = new NodeFile
                {
                    ValueIndex = node.ValueIndex,
                    ChildrenCount = (short)(node.Children != null ? (node.Children.Where(m=>m!=null).Count()) : 0),
                    
                    StartGeoInfo_Address = node.StartGeoInfo_Address,
                    EndGeoInfo_Address = node.EndGeoInfo_Address

                };

                
                // Calculate children address - point to the first child
                if (node.Children != null && node.Children.Length > 0 )
                {
                    for (int i = 0; i < node.Children.Count(); ++i)
                    {
                        if(node.Children[i]!=null)
                        {
                            nodeFile.Children_Address = nodeAddresses[node.Children[i]];
                            break;
                        }
                    }
                        
                }
                else
                {
                    nodeFile.Children_Address = 0;
                }

                WriteNodeFile(bw, nodeFile);
               
                node.test = 1;
            }
        }

        return levelAddresses;
    }
    Node[] Write_GeoInfoToFile(Node[] nodes, FileStream fs, BinaryWriter bw)
    {
        Node[] new_nodes = nodes;
        for (int i=0;i< nodes.Length;++i)
        {
            if (nodes[i] != null && nodes[i].info != null)
            {
               
                new_nodes[i].StartGeoInfo_Address = fs.Position;

                // Create a single string and write it
                string geoData = $"{nodes[i].info.CountryCode};{nodes[i].info.CountryName};{nodes[i].info.StateCode};{nodes[i].info.StateName}";

                // Write as simple byte array (no length prefix)
                byte[] dataBytes = Encoding.UTF8.GetBytes(geoData);
                bw.Write(dataBytes);

               
                new_nodes[i].EndGeoInfo_Address = fs.Position;
            }

        }

        for (int i = 0; i < nodes.Length; ++i)
        {
           
            if (nodes[i] != null)
                if (nodes[i].Children != null && nodes[i].Children.Length > 0)
                {
                    Write_GeoInfoToFile(nodes[i].Children, fs, bw);
                }

        }
        return new_nodes;
    }
  

    void WriteNodeFile(BinaryWriter bw, NodeFile nodeFile)
    {
        bw.Write(nodeFile.ValueIndex);
        bw.Write(nodeFile.ChildrenCount);
        bw.Write(nodeFile.Children_Address);
        bw.Write(nodeFile.StartGeoInfo_Address);
        bw.Write(nodeFile.EndGeoInfo_Address);
    }


}

class ReadFromFile
{
    public void Search(string ip_s)
    {
        // Type of file (V4 or V6)
        string filePath;
        byte[] ipBytes;

        if (IPAddress.TryParse(ip_s, out IPAddress ip))
        {
            if (ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
            {
                filePath = "mmap-IpV4";
                ipBytes = ip.GetAddressBytes();
                Console.WriteLine($"Searching in IPv4 database: {filePath}");
            }
            else if (ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetworkV6)
            {
                filePath = "mmap-IpV6";
                ipBytes = ip.GetAddressBytes();
                Console.WriteLine($"Searching in IPv6 database: {filePath}");
            }
            else
            {
                Console.WriteLine("Unsupported IP address family");
                return;
            }
        }
        else
        {
            Console.WriteLine("Invalid IP address format");
            return;
        }

        if (!File.Exists(filePath))
        {
            Console.WriteLine($"Error://**//**//**//** Database file not found: {filePath} **//**//**//**\n");
            return;
        }


        using var mmf = MemoryMappedFile.CreateFromFile(filePath, FileMode.Open, null, 0, MemoryMappedFileAccess.Read);
        using var acc = mmf.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read);
       
        // read header
        BinaryFileHeader header = new();
        header.GeoInfoOffset = acc.ReadInt64(0);
        header.NodesOffset = acc.ReadInt64(8);
       

        // read nodes
        var AddressofGeo = GetAddressOfGeo( acc, IpToUint(ip_s), level:0, header.NodesOffset, 1,true);

        // read Geo info
        if (AddressofGeo != null && AddressofGeo.StartGeoInfo_Address != -1)
        {
            var geoInfo = ReadGeoInfoFromFile(acc, AddressofGeo.StartGeoInfo_Address, AddressofGeo.EndGeoInfo_Address);
            if (geoInfo != null)
            {
                Console.WriteLine($"Found: Country code= {geoInfo.CountryCode}, Country={geoInfo.CountryName}, State code= {geoInfo.StateCode}, State={geoInfo.StateName}");
            }
            else
            {
                Console.WriteLine("No geo information found for this IP");
            }
        }
        else
        {
            Console.WriteLine("IP address not found in database");
        }

    }
      
        

    
    private NodeFile ReadNodeFileStruct(MemoryMappedViewAccessor acc,long address)
    {
        NodeFile nodeFile = new NodeFile();

    // Read fields in the same order they were written
        nodeFile.ValueIndex = acc.ReadInt32(address);
        nodeFile.ChildrenCount = acc.ReadInt32(address+4);
        nodeFile.Children_Address = acc.ReadInt64(address+8);
        nodeFile.StartGeoInfo_Address = acc.ReadInt64(address + 16);
        nodeFile.EndGeoInfo_Address = acc.ReadInt64(address + 24);

        return nodeFile;
    }
    private GeoInfo ReadGeoInfoFromFile(MemoryMappedViewAccessor acc, long startAddress,long endAddress)
    {
        int dataLength = (int)(endAddress - startAddress);
        byte[] buffer = new byte[dataLength];
        acc.ReadArray(startAddress, buffer, 0, dataLength);

        string geoData = Encoding.UTF8.GetString(buffer);
        string[] parts = geoData.Split(';');

        if (parts.Length == 4)
        {
            return new GeoInfo(parts[0], parts[1], parts[2], parts[3]);
        }


        return null ;
    }
    public byte[] IpToUint(string ipString)
    {
        if (!IPAddress.TryParse(ipString, out IPAddress ip))
            throw new ArgumentException("Invalid IP address format");

        return ip.GetAddressBytes();
    }
    NodeFile? GetAddressOfGeo(MemoryMappedViewAccessor acc, byte[] ip, int level,long startAddress,int nodesCount,bool isRoot)
    {

        // bytes big-endian order: b0 b1 b2 b3
        //byte b0 = (byte)(ip >> 24);
        //byte b1 = (byte)(ip >> 16);
        //byte b2 = (byte)(ip >> 8);
        //byte b3 = (byte)ip;

        //Span<byte> bytes = stackalloc byte[4] { b0, b1, b2, b3};

        if (level >= ip.Length)
            return null;

        NodeFile nodefile =new NodeFile { StartGeoInfo_Address = -1, EndGeoInfo_Address=-1 };

        long AddressofGeo = 0;
        uint smallest = 255;
        for(int i=0;i<nodesCount;++i)
        {
            // read each node in start address to end of nodes
            var currentNode = ReadNodeFileStruct(acc, startAddress+i*32);
            if (currentNode.ValueIndex != -1)
            {
                var IpDistance = ip[level] - currentNode.ValueIndex;
                // if (the byte for this level - the index value from node)= 0; smallest=0;  nodefile = this node break;
                if (IpDistance == 0)
                {
                    smallest = 0;
                    nodefile = currentNode;
                    break;
                }
                // if (the byte for this level - the index value from node)< smallest; smallest= result; nodefile = this node
                if (IpDistance < smallest && IpDistance > 0)
                {
                    smallest = (uint)IpDistance;
                    nodefile = currentNode;
                }
            }
            if(isRoot)
            {
                nodefile = currentNode;
            }
        }

        if (smallest == 0 || isRoot)
        {
            var nodefile_ = GetAddressOfGeo(acc, ip,isRoot?level:++level, nodefile.Children_Address, nodefile.ChildrenCount, false);
            if (nodefile_ != null)
                return nodefile_;
        }

        // 
        if (smallest!=0 && smallest>0 && smallest!=255)
        {
            return nodefile;
        }

        if(smallest<0 || smallest==255)
        {
            return null;
        }
       

        return nodefile;

    }
}


class Program
{
    static void Main()
    {
        BuildOfBinaryFile createFile = new BuildOfBinaryFile();

        // If IpV4 memory-map file isn't exist create it
        if (!File.Exists("mmap-IpV4"))
            createFile.Build("geo-US.csv", "mmap-IpV4", "mmap-IpV6");

        ReadFromFile data = new ReadFromFile();
     

        Console.WriteLine("=== IPv4 Test ===");
        data.Search("96.38.13.10");
        data.Search("74.142.241.58");
        data.Search("2.56.188.2");

        Console.WriteLine("\n=== IPv6 Test ===");
        // 
        data.Search("2600:4809:7240:2000:1000:0:0:0");
        data.Search("2a02:26f7:d15d:8819:82:19:10:0");
        data.Search("2606:4700:4700::1111");

        Console.ReadLine();
    }

}
  