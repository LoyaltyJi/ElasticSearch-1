package elasticsearch;

/**
 *  ESIndexer indexes the documents present in src/main/resources/ap89_collection.
 *  
 *  /ap_dataset is created(done manually) in ElasticSearch and docno and text for each document
 *  are indexed.
 *  
 *  Dependencies are resolved with Maven
 *  
 *  @author Amod Samant
 */

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;

public class ESIndexer {
	
	public static int id = 0;

	public static void main(String[] args) throws IOException {
		
		Node node = nodeBuilder().node();
		Client client = node.client();
		
		String docNo = "";
		String textSectionString;
		File resourceLocation = new File("src/main/resources/ap89_collection");
		File[] files = resourceLocation.listFiles();
		
		for (File fileToIndex : files) {
			BufferedReader bufferedReader = new BufferedReader(new FileReader(fileToIndex));
			String line; 
			textSectionString = "";
			while((line = bufferedReader.readLine())!=null) {
				
				if(line.contains("<DOCNO>") && line.contains("</DOCNO>")) {
					docNo = line.replaceAll("<DOCNO>\\s+(.+?)\\s+</DOCNO>", "$1");
					id++;
				}
				
				if(line.contains("<TEXT>")) {
					StringBuilder textSection = new StringBuilder();
					
					while (!((line = bufferedReader.readLine()).contains("</TEXT>"))) {
						textSection.append(line).append(" ");
					}  
					textSectionString = textSectionString + textSection.toString();
				}
				
				if(line.equals("</DOC>")) {
					
					XContentBuilder builder = 
							jsonBuilder().startObject()
							.field("docno",docNo)
							.field("text",textSectionString)
							.endObject();
							
					 
					client.prepareIndex("ap_dataset", "document",String.valueOf(id))
					.setSource(builder)
					.execute()
					.actionGet();
					
					textSectionString = "";
					System.out.println(id);
					builder.close();	
				}
			}
		bufferedReader.close();	
		}
		client.close();
		node.close();
		
	}

}
