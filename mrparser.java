import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.bzip2.CBZip2InputStream;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.w3c.dom.CharacterData;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;


public class mrparser 
{
public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

Pattern title = Pattern.compile("(<title>)(.+?)(<\\/title>)");
//Pattern store = Pattern.compile(".*(<text([^>]*)>(.*^\n)</text>).*");
@Override
public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)throws IOException {
String textop = new String();
String newone = new String();
try{
String xmlRecords = value.toString();
DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
   InputSource is = new InputSource();
   is.setCharacterStream(new StringReader(xmlRecords));

   Document doc = db.parse(is);
   NodeList nodes = doc.getElementsByTagName("page");

   for (int i = 0; i < nodes.getLength(); i++) {
     Element element = (Element) nodes.item(i);

     NodeList name = element.getElementsByTagName("text");
     Element line = (Element) name.item(0);
                      textop = new String(getCharacterDataFromElement(line));
    
  
    Matcher m = title.matcher(xmlRecords);
    if(m.find()){
    textop=textop.replaceAll("\n", " ");
    newone=new String(m.group(2)+",");
    output.collect(new Text(newone), new Text(textop));
    }
}
    xmlRecords = null;
}
catch(Exception e)
{
e.getStackTrace();
}
}
 
}
public static void main(String[] args) throws Exception{
JobConf conf = new JobConf();
StringBuffer str = new StringBuffer();
String abc="https://s3.amazonaws.com/cs9223/enwiki-20121001/enwiki-20121001-pages-meta-current1.xml-p000000010p000010000.bz2,https://s3.amazonaws.com/cs9223/enwiki-20121001/enwiki-20121001-pages-meta-current2.xml-p000010001p000025000.bz2,https://s3.amazonaws.com/cs9223/enwiki-20121001/enwiki-20121001-pages-meta-current3.xml-p000025001p000055000.bz2,https://s3.amazonaws.com/cs9223/enwiki-20121001/enwiki-20121001-pages-meta-current4.xml-p000055002p000104998.bz2,https://s3.amazonaws.com/cs9223/enwiki-20121001/enwiki-20121001-pages-meta-current5.xml-p000105001p000184999.bz2,https://s3.amazonaws.com/cs9223/enwiki-20121001/enwiki-20121001-pages-meta-current6.xml-p000185003p000305000.bz2,https://s3.amazonaws.com/cs9223/enwiki-20121001/enwiki-20121001-pages-meta-current7.xml-p000305002p000464997.bz2,https://s3.amazonaws.com/cs9223/enwiki-20121001/enwiki-20121001-pages-meta-current8.xml-p000465001p000665000.bz2,https://s3.amazonaws.com/cs9223/enwiki-20121001/enwiki-20121001-pages-meta-current9.xml-p000665001p000925000.bz2,https://s3.amazonaws.com/cs9223/enwiki-20121001/enwiki-20121001-pages-meta-current10.xml-p000925001p001325000.bz2,https://s3.amazonaws.com/cs9223/enwiki-20121001/enwiki-20121001-pages-meta-current11.xml-p001325001p001825000.bz2,https://s3.amazonaws.com/cs9223/enwiki-20121001/enwiki-20121001-pages-meta-current12.xml-p001825001p002425000.bz2,https://s3.amazonaws.com/cs9223/enwiki-20121001/enwiki-20121001-pages-meta-current13.xml-p002425001p003124998.bz2,https://s3.amazonaws.com/cs9223/enwiki-20121001/enwiki-20121001-pages-meta-current14.xml-p003125001p003924999.bz2,https://s3.amazonaws.com/cs9223/enwiki-20121001/enwiki-20121001-pages-meta-current15.xml-p003925001p004825000.bz2,https://s3.amazonaws.com/cs9223/enwiki-20121001/enwiki-20121001-pages-meta-current16.xml-p004825002p006025000.bz2,https://s3.amazonaws.com/cs9223/enwiki-20121001/enwiki-20121001-pages-meta-current17.xml-p006025001p007524997.bz2,https://s3.amazonaws.com/cs9223/enwiki-20121001/enwiki-20121001-pages-meta-current18.xml-p007525002p009225000.bz2,https://s3.amazonaws.com/cs9223/enwiki-20121001/enwiki-20121001-pages-meta-current19.xml-p009225001p011125000.bz2,https://s3.amazonaws.com/cs9223/enwiki-20121001/enwiki-20121001-pages-meta-current20.xml-p011125001p013324998.bz2,https://s3.amazonaws.com/cs9223/enwiki-20121001/enwiki-20121001-pages-meta-current21.xml-p013325001p015725000.bz2,https://s3.amazonaws.com/cs9223/enwiki-20121001/enwiki-20121001-pages-meta-current22.xml-p015725003p018225000.bz2,https://s3.amazonaws.com/cs9223/enwiki-20121001/enwiki-20121001-pages-meta-current23.xml-p018225001p020925000.bz2,https://s3.amazonaws.com/cs9223/enwiki-20121001/enwiki-20121001-pages-meta-current24.xml-p020925002p023725000.bz2,https://s3.amazonaws.com/cs9223/enwiki-20121001/enwiki-20121001-pages-meta-current25.xml-p023725001p026625000.bz2,https://s3.amazonaws.com/cs9223/enwiki-20121001/enwiki-20121001-pages-meta-current26.xml-p026625002p029625000.bz2,https://s3.amazonaws.com/cs9223/enwiki-20121001/enwiki-20121001-pages-meta-current27.xml-p029625001p037187679.bz2";
String inp[] = abc.split(",");
String allInputFiles[] = new String[inp.length];
String outputLocation = args[0];
int i = 0;
System.out.println("Downloading file(s)...");
for(i = 0; i<inp.length;i++)
{
allInputFiles[i] = new String(args[1]+"/input_file_"+i);
mrparser.unzip(inp[i], conf,allInputFiles[i]);
str.append(new Path(allInputFiles[i]));
str.append(",");
}
str.deleteCharAt(str.length()-1);
conf.setJobName("MrParser");
conf.setJarByClass(mrparser.class);
conf.setOutputKeyClass(Text.class);
conf.setOutputValueClass(Text.class);
conf.setMapperClass(Map.class);
conf.setNumReduceTasks(0);
conf.setInputFormat(XmlInputFormat.class);
conf.set(XmlInputFormat.START_TAG_KEY, "<page>") ;
conf.set(XmlInputFormat.END_TAG_KEY, "</page>") ;
FileInputFormat.setInputPaths(conf,  str.toString());
FileOutputFormat.setOutputPath(conf, new Path(outputLocation));
JobClient.runJob(conf);
  
}
public static String getCharacterDataFromElement(Element e) {
   Node child = e.getFirstChild();
   if (child instanceof CharacterData) {
     CharacterData cd = (CharacterData) child;
     return cd.getData();
   }
   return "";
 }
public static void unzip(String urlLocation, JobConf conf, String inputLocation) throws IOException, URISyntaxException{
URI uri = new URI(inputLocation);
URL url = new URL(urlLocation);
InputStream is = url.openStream();
FileSystem fs = FileSystem.get(uri,conf);
Path path = new Path(inputLocation);
if(fs.exists(path)){
fs.delete(path, false);
}
is.read();
is.read();
CBZip2InputStream bzis = new CBZip2InputStream(is);
FSDataOutputStream  fsos = fs.create(path, true);
int singleLetter;
while ((singleLetter=bzis.read()) != -1){ 
 fsos.write(singleLetter);
}
fsos.flush(); 
fsos.close();
is.close();
bzis.close(); 
}
}