package elasticsearch;

/**
 * 	ESQueryExecution reads the query file and executes the queries on
 *  the /ap_dataset index.
 *  
 *  This file requires ESIndexer to be run only once before.
 *  
 *  @author Amod Samant
 */
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;

public class ESQueryProcessor {
	
	public static void main(String[] args) throws IOException {
	
		HashSet<String> hashUselessWords = ESUtils.createHashSetStopAndExtra();
		List<String> queryWords = Lists.newArrayList();
		File queryFile = new File("src/main/resources/query_desc.51-100.short.txt");
		
		BufferedReader bufferedReader = new BufferedReader(new FileReader(queryFile));
		String queryLine; 
		String queryNum;
		
		HashMap<String,Double> docNumLengthMap = Maps.newHashMap();
		HashMap<String,Double> termFreqCorpus = Maps.newHashMap();
		
		HashMap<String,Double> okaptfScoreQueryPerDoc = Maps.newHashMap();
		HashMap<String,Double> tfIdfScoreQueryPerDoc = Maps.newHashMap();
		HashMap<String,Double> okapiBM25ScoreQueryPerDoc = Maps.newHashMap();
		HashMap<String,Double> uniLaplaceScoreQueryPerDoc = Maps.newHashMap();
		HashMap<String,Double> uniJMScoreQueryPerDoc = Maps.newHashMap();
		
		HashMap<String,List<String>> lmHelper = Maps.newHashMap();
		
		Node node = nodeBuilder().node();
		Client client = node.client();
		
		double V = ESUtils.vocabSize(client);
		double avgDocLength = ESUtils.avgDocLengthCountFunction(client,"avgLength");
		double numOfDocs = ESUtils.avgDocLengthCountFunction(client,"totalDocs");
		double docLengthC = ESUtils.avgDocLengthCountFunction(client, "lengthOfCorpus");
		
		File okaptfFile = new File("output/okaptf.txt");
		if (okaptfFile.exists())
			okaptfFile.delete();
		okaptfFile.createNewFile();
		
		File tfIdfFile = new File("output/tfidf.txt");
		if (tfIdfFile.exists())
			tfIdfFile.delete();
		tfIdfFile.createNewFile();
		
		File okapiBM25File = new File("output/okapiBM25.txt");
		if (okapiBM25File.exists())
			okapiBM25File.delete();
		okapiBM25File.createNewFile();
		
		
		File uniLaplaceFile = new File("output/uniLaplace.txt");
		if (uniLaplaceFile.exists()) 
			uniLaplaceFile.delete();
		uniLaplaceFile.createNewFile();
		
		File uniJMFile = new File("output/uniJM.txt");
		if (uniJMFile.exists()) 
			uniJMFile.delete();
		uniJMFile.createNewFile();
		
		
		while((queryLine = bufferedReader.readLine())!=null) {
			StringBuilder queryString = new StringBuilder();
			//Remove ending empty lines
			queryLine = queryLine.trim();
			if(queryLine.equals(""))
				continue;
			queryNum = queryLine.replaceAll("^([0-9]+).*", "$1");
			System.out.println(queryNum);
			
			queryLine = queryLine.replaceAll("\\,(\\s)|\\.$", " ");
			
			String[] splitQuery = queryLine.split("^[0-9]+\\.\\s+(Document)\\s([a-z]+)\\s([a-z]+) ");

			for(String term: splitQuery[1].split(" ")) {
				if(!hashUselessWords.contains(term))
					queryWords.add(term);
			}
			
			for(String term: queryWords) {
				queryString.append(term).append(" ");
			}
			
			System.out.println(queryString.toString());
			
			HashMap<String,Double> termFreqQueryMap = ESUtils.termFreqInQuery(queryString.toString());
			
			for(Map.Entry<String, Double> termKey : termFreqQueryMap.entrySet()) {
				
				String term = termKey.getKey();
				double tfq = termFreqQueryMap.get(term);
				
				CountResponse countResponse = client.prepareCount("ap_dataset")
				        .setQuery(matchQuery("text", term))
				        .execute()
				        .actionGet();
				double docFreq = countResponse.getCount();
				SearchResponse response = client.prepareSearch("ap_dataset")
				        .setTypes("document")
				        .setQuery(QueryBuilders.matchQuery("text", term))
				        .setFrom(0).setSize((int)docFreq).setExplain(true)
				        .execute()
				        .actionGet();
				
				SearchHit[] responseString = response.getHits().getHits();
				
				if(!termFreqCorpus.containsKey(term)) {
					double tfdC = ESUtils.calculateTermFreqCorpus(responseString);
					termFreqCorpus.put(term, tfdC);
				}
				
				
				for (SearchHit searchHit : responseString) {
					
					double tfd = 0.0;
					String id = searchHit.getId();
					String docno = searchHit.getSource().values().toArray()[1].toString(); // docno
				
					String termFreq = searchHit.getExplanation().getDetails()[0].toString();
					
					Pattern termFreqPattern = Pattern.compile("termFreq=([0-9]+).0");
					Matcher termFreqMatcher = termFreqPattern.matcher(termFreq);
					if (termFreqMatcher.find()){
						tfd = Double.parseDouble(termFreqMatcher.group(1));;
					}					
					if(!docNumLengthMap.containsKey(docno)) {
						double docLength = ESUtils.docLengthFunction(id,client);
						docNumLengthMap.put(docno, docLength);
					}
					
					double okaptfScore = ESModels.okaptf(tfd,docNumLengthMap.get(docno),avgDocLength);
					double tfIdfScore = ESModels.tfIdf(okaptfScore, numOfDocs, docFreq);
					double okapiBM25Score = ESModels.okapiBM25(tfd, tfq, numOfDocs, docFreq, docNumLengthMap.get(docno), avgDocLength);
					double uniLaplaceScore = ESModels.unigramLMLaplaceSmoothing(tfd, docNumLengthMap.get(docno), V);
					double uniJMScore = ESModels.unigramLMJelinekMercerSmoothing(tfd, avgDocLength, termFreqCorpus.get(term), docLengthC); //tfdC

					
					if(!okaptfScoreQueryPerDoc.containsKey(docno)) {
						okaptfScoreQueryPerDoc.put(docno, okaptfScore);
						tfIdfScoreQueryPerDoc.put(docno, tfIdfScore);
						okapiBM25ScoreQueryPerDoc.put(docno, okapiBM25Score);
						
						uniLaplaceScoreQueryPerDoc.put(docno, uniLaplaceScore);
						
						List<String> tempList = new ArrayList<String>();
						tempList.add(term);
						List<String> lmListPerDoc = tempList;
						lmHelper.put(docno, lmListPerDoc);
						
						uniJMScoreQueryPerDoc.put(docno, uniJMScore);
					} else {
						double storedOkaptfScore = okaptfScoreQueryPerDoc.get(docno);
						okaptfScoreQueryPerDoc.put(docno, storedOkaptfScore + okaptfScore);
						
						double storedTfIdfScore = tfIdfScoreQueryPerDoc.get(docno);
						tfIdfScoreQueryPerDoc.put(docno, storedTfIdfScore + tfIdfScore);
						
						double storedOkapiBM25Score = okapiBM25ScoreQueryPerDoc.get(docno);
						okapiBM25ScoreQueryPerDoc.put(docno, storedOkapiBM25Score + okapiBM25Score);
						
						double storedUniLaplaceScore = uniLaplaceScoreQueryPerDoc.get(docno);
						uniLaplaceScoreQueryPerDoc.put(docno, storedUniLaplaceScore + uniLaplaceScore);
						
						List<String> tempList = new ArrayList<String>();
						tempList = lmHelper.get(docno);
						tempList.add(term);
						lmHelper.put(docno, tempList);
						
						double storedUniJMfScore = uniJMScoreQueryPerDoc.get(docno);
						uniJMScoreQueryPerDoc.put(docno, storedUniJMfScore + uniJMScore);
						
					}
					
				}
			
			}
			
			// Update score for non-occurring terms in the documents
			List<HashMap<String,Double>> lmMaps = ESModels.lmNonTerms(lmHelper,
					uniLaplaceScoreQueryPerDoc,
					uniJMScoreQueryPerDoc,
					termFreqQueryMap,
					termFreqCorpus,
					docNumLengthMap,
					docLengthC,
					V);

			uniLaplaceScoreQueryPerDoc = lmMaps.get(0);
			uniJMScoreQueryPerDoc = lmMaps.get(1);
			
			// Sort according to score
			List<HashMap.Entry<String,Double>> sortedOkaptfScore = Lists.newLinkedList(okaptfScoreQueryPerDoc.entrySet());
			List<HashMap.Entry<String,Double>> sortedTfIdfScore = Lists.newLinkedList(tfIdfScoreQueryPerDoc.entrySet());
			List<HashMap.Entry<String,Double>> sortedOkapiBM25Score = Lists.newLinkedList(okapiBM25ScoreQueryPerDoc.entrySet());
			List<HashMap.Entry<String,Double>> sortedUniLaplaceScore = Lists.newLinkedList(uniLaplaceScoreQueryPerDoc.entrySet());
			List<HashMap.Entry<String,Double>> sortedUniJMScore = Lists.newLinkedList(uniJMScoreQueryPerDoc.entrySet());
			
			Collections.sort(sortedOkaptfScore, new Comparator<HashMap.Entry<String,Double>>(){
				public int compare(HashMap.Entry<String,Double> score1,HashMap.Entry<String,Double> score2) {
					return score2.getValue().compareTo(score1.getValue());
				}
			});
			Collections.sort(sortedTfIdfScore, new Comparator<HashMap.Entry<String,Double>>(){
				public int compare(HashMap.Entry<String,Double> score1,HashMap.Entry<String,Double> score2) {
					return score2.getValue().compareTo(score1.getValue());
				}
			});
			Collections.sort(sortedOkapiBM25Score, new Comparator<HashMap.Entry<String,Double>>(){
				public int compare(HashMap.Entry<String,Double> score1,HashMap.Entry<String,Double> score2) {
					return score2.getValue().compareTo(score1.getValue());
				}
			});
			Collections.sort(sortedUniLaplaceScore, new Comparator<HashMap.Entry<String,Double>>(){
				public int compare(HashMap.Entry<String,Double> score1,HashMap.Entry<String,Double> score2) {
					return score2.getValue().compareTo(score1.getValue());
				}
			});
			Collections.sort(sortedUniJMScore, new Comparator<HashMap.Entry<String,Double>>(){
				public int compare(HashMap.Entry<String,Double> score1,HashMap.Entry<String,Double> score2) {
					return score2.getValue().compareTo(score1.getValue());
				}
			});
			
			FileWriter okaptfWriter = new FileWriter(okaptfFile,true);
			BufferedWriter okaptfBufWriter = new BufferedWriter(okaptfWriter);
			int rank=1;
			for (Entry<String, Double> list : sortedOkaptfScore) {
				okaptfBufWriter.write(queryNum+" Q0 "+list.getKey()+" "+ rank++ +" "+list.getValue()+" Exp\n");
				if(rank==1001) {
					break;
				}
			}
			okaptfBufWriter.close();
			
			FileWriter tfIdfWriter = new FileWriter(tfIdfFile,true);
			BufferedWriter tfIdfBufWriter = new BufferedWriter(tfIdfWriter);
			rank=1;
			for (Entry<String, Double> list : sortedTfIdfScore) {
				tfIdfBufWriter.write(queryNum+" Q0 "+list.getKey()+" "+ rank++ +" "+list.getValue()+" Exp\n");
				if(rank==1001) {
					break;
				}
			}
			tfIdfBufWriter.close();
			
			FileWriter okapiBM25Writer = new FileWriter(okapiBM25File,true);
			BufferedWriter okapiBM25BufWriter = new BufferedWriter(okapiBM25Writer);
			rank=1;
			for (Entry<String, Double> list : sortedOkapiBM25Score) {
				okapiBM25BufWriter.write(queryNum+" Q0 "+list.getKey()+" "+ rank++ +" "+list.getValue()+" Exp\n");
				if(rank==1001) {
					break;
				}
			}
			okapiBM25BufWriter.close();

			FileWriter uniLaplaceWriter = new FileWriter(uniLaplaceFile,true);
			BufferedWriter uniLaplaceBufWriter = new BufferedWriter(uniLaplaceWriter);
			rank=1;
			for (Entry<String, Double> list : sortedUniLaplaceScore) {
				uniLaplaceBufWriter.write(queryNum+" Q0 "+list.getKey()+" "+ rank++ +" "+list.getValue()+" Exp\n");
				if(rank==1001) {
					break;
				}
			}
			uniLaplaceBufWriter.close();

			FileWriter uniJMWriter = new FileWriter(uniJMFile,true);
			BufferedWriter uniJMBufWriter = new BufferedWriter(uniJMWriter);
			rank=1;
			for (Entry<String, Double> list : sortedUniJMScore) {
				uniJMBufWriter.write(queryNum+" Q0 "+list.getKey()+" "+ rank++ +" "+list.getValue()+" Exp\n");
				if(rank==1001) {
					break;
				}
			}
			uniJMBufWriter.close();
			
			queryString.setLength(0);
			queryWords.clear();
			
			okaptfScoreQueryPerDoc.clear();
			tfIdfScoreQueryPerDoc.clear();
			okapiBM25ScoreQueryPerDoc.clear();
			uniLaplaceScoreQueryPerDoc.clear();
			uniJMScoreQueryPerDoc.clear();
			
			lmHelper.clear();
			termFreqQueryMap.clear();
		}
		
		node.close();
		client.close();
		bufferedReader.close();
	
	}
}