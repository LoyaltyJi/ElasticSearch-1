package elasticsearch;

/**
 * 	Contains all the utility functions for retrieving information
 *  from Elastic Search. Also contains some query processing 
 *  functions
 *  
 *  @author Amod Samant
 */
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.index.Fields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.termvector.TermVectorResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.metrics.cardinality.InternalCardinality;
import org.elasticsearch.search.facet.statistical.StatisticalFacet;

public class ESUtils {

	/*
	 * Function to create a HashSet for all the stopwords. File stoplist.txt 
	 * needs to exist at src/main/resources. 
	 *  
	 */
	static HashSet<String> createHashSetStopAndExtra() throws IOException {
		
		HashSet<String> hashUselessWords = Sets.newHashSet();
		File stopWordsFile = new File("src/main/resources/stoplist.txt");
		
		BufferedReader bufferedReader = new BufferedReader(new FileReader(stopWordsFile));
		String line;
		while((line = bufferedReader.readLine())!=null) {
			hashUselessWords.add(line);
		}
		bufferedReader.close();
		
		return hashUselessWords;
	}

	/*
	 * Function to calculate document length for the given doument id
	 */
	static double docLengthFunction(String id, Client client) throws IOException {
		
		double docLength = 0;
		TermVectorResponse termVector = 
		client.prepareTermVector("ap_dataset","document",id)
		.setTermStatistics(true)
		.setFieldStatistics(true)
		.setOffsets(true)
		.setPayloads(true)
		.setPositions(true)
		.setSelectedFields("text")
		.execute()
		.actionGet();
		
		Fields fields = termVector.getFields();
		for(String field : fields) {
		    Terms terms = fields.terms(field);
		    TermsEnum termsEnum = terms.iterator(null);
		    
		    BytesRef text;
		    while((text = termsEnum.next()) != null) {
		    	docLength = docLength + termsEnum.docs(null, null).freq(); // term frequency
		    }
		}
		
		return docLength;
	}
	
	/*
	 * Function to retrieve document metrics as per the given type.
	 */
	static double avgDocLengthCountFunction(Client client, String type) throws IOException {
		
		XContentBuilder facetBuilder = 
				jsonBuilder().startObject()
				.startObject("query")
					.startObject("match_all")
					.endObject()
				.endObject()
				.startObject("facets")
					.startObject("text")
						.startObject("statistical")
								.field("script","doc['text'].values.size()")
						.endObject()
					.endObject()
				.endObject()
				.endObject();
		SearchResponse statResponse = 
				client.prepareSearch("ap_dataset").setTypes("document").setSource(facetBuilder)
				.execute()
				.actionGet();
	
		StatisticalFacet statFacet = (StatisticalFacet) statResponse.getFacets().facetsAsMap().get("text");
		if(type.equalsIgnoreCase("avgLength")) {
			return statFacet.getMean();
		} else if (type.equalsIgnoreCase("lengthOfCorpus")) {
			return statFacet.getTotal();
		} else {
			return statFacet.getCount();
		}
	}
	
	/*
	 * Function to create a HashMap of Term Frequencies in a Query
	 */
	static HashMap<String,Double> termFreqInQuery(String query) {
		
		HashMap<String,Double> termFreqQueryMap = Maps.newHashMap();
		
		String[] splitQuery = query.split(" ");
		for(String term: splitQuery) {
			if(termFreqQueryMap.containsKey(term)) {
				Double countTerm = termFreqQueryMap.get(term);
				termFreqQueryMap.put(term, countTerm + 1.0);
			} else {
				termFreqQueryMap.put(term,1.0);
			}
		}
		return termFreqQueryMap;	
	}
	
	/*
	 * Function to calculate term frequency in the whole corpus
	 */
	static double calculateTermFreqCorpus(SearchHit[] responseString) {
		
		double termFreq=0.0;
		
		Pattern termFreqPattern = Pattern.compile("termFreq=([0-9]+).0");
		
		for (SearchHit searchHit: responseString) {
			String termFreqString = searchHit.getExplanation().getDetails()[0].toString();
			Matcher termFreqMatcher = termFreqPattern.matcher(termFreqString);
			if (termFreqMatcher.find()){
				termFreq = termFreq + Double.parseDouble(termFreqMatcher.group(1)); // Term Frequency TF
			}
		}
		return termFreq;
	}
	
	/*
	 * Function to get the Vocabulary Size
	 */
	static double vocabSize(Client client) throws IOException {
		
		XContentBuilder aggsBuilder = 
				jsonBuilder().startObject()
				.startObject("aggs")
					.startObject("unique_terms")
						.startObject("cardinality")
								.field("script","doc['text'].values")
						.endObject()
					.endObject()
				.endObject()
				.endObject();
		SearchResponse statResponse = 
				client.prepareSearch("ap_dataset").setSource(aggsBuilder)
				.execute()
				.actionGet();
		
		InternalCardinality statAgg = (InternalCardinality)statResponse.getAggregations().asList().get(0);
		return statAgg.getValue();
	}
	
}
