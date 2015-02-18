package elasticsearch;

/**
 *  Contains all the model functions
 *  
 *  @author Amod Samant
 */
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ESModels {

	static final double lambda = 0.5;
	static final double k1 = 1.2;
	static final double k2 = 100;
	static final double b = 0.75;
	
	/*
	 * Okapi TF Model
	 */
	static double okaptf(double tf, double docLength, double avgDocLength) {
		
		double okaptfScore= 0.0;
		okaptfScore = tf/(tf+0.5+1.5*(docLength/avgDocLength));
		return okaptfScore;
	}
	
	/*
	 * TF-IDF Model
	 */
	static double tfIdf(double okapTFScore, double numOfDocs, double docFreq) {
		
		double tfIdfScore= 0.0;
		tfIdfScore = okapTFScore*Math.log(numOfDocs/docFreq);
		return tfIdfScore;
	}
	
	/*
	 * Okapi BM25 Model
	 */
	static double okapiBM25(double tfd, double tfq, double numOfDocs, double docFreq, double docLength, double avgDocLength) {
		
		double okapiBM25Score= 0.0;
		okapiBM25Score = Math.log((numOfDocs+0.5)/(docFreq+0.5)) *
						 ((tfd+k1*tfd)/(tfd+k1*((1-b)+b*(docLength/avgDocLength))))*
						 ((tfq+k2*tfq)/(tfq+k2));
		return okapiBM25Score;
	}
	
	/*
	 * Unigram Laplace Smoothing Model
	 */
	static double unigramLMLaplaceSmoothing(double tfd, double docLength,double V) {
		
		double unigramLMLaplaceSmoothingScore= 0.0;
		unigramLMLaplaceSmoothingScore = Math.log((tfd+1)/(docLength+V));
		return unigramLMLaplaceSmoothingScore;
	}
	
	/*
	 * Unigram Jelinek-Mercer Smoothing Model
	 */
	static double unigramLMJelinekMercerSmoothing(double tfd, double docLength, double tfdC, double docLengthC) {
		
		double unigramLMJelinekMercerSmoothingScore= 0.0;
		
		unigramLMJelinekMercerSmoothingScore = Math.log((lambda*tfd/docLength)+
												((1-lambda)*tfdC/docLengthC));
		
		return unigramLMJelinekMercerSmoothingScore;
	}
	
	/*
	 *  Function to add score for terms not existing in documents.
	 *  Only for Language models.
	 */
	static List<HashMap<String,Double>> lmNonTerms(HashMap<String,List<String>> lMHelper, 
			HashMap<String,Double> uniLaplaceScoreQueryPerDoc,
			HashMap<String,Double> uniJMScoreQueryPerDoc,
			HashMap<String,Double> termFreqQueryMap,
			HashMap<String,Double> termFreqCorpus,
			HashMap<String,Double> docLengthMap,
			double docLengthC,
			double V) {
		
		List<HashMap<String,Double>> lmMaps = new ArrayList<HashMap<String,Double>>(); 
		
		HashMap<String,Double> uniJM = uniJMScoreQueryPerDoc;
		HashMap<String,Double> uniLaplace = uniLaplaceScoreQueryPerDoc;
		
		for(Map.Entry<String, List<String>> doc : lMHelper.entrySet()) {
			
			List<String> lmDocTerms = doc.getValue();
			String docno = doc.getKey();
	
			double finalLapScore = 0.0;
			double finalJMScore = 0.0;
			
			double jmScore = uniJM.get(docno);
			double laplaceScore = uniLaplace.get(docno);
			
			// Calculating Laplace and JM scores
			for(Map.Entry<String,Double> term : termFreqQueryMap.entrySet()) {
				
				if(!lmDocTerms.contains(term.getKey())) {
					
					double tfdC = termFreqCorpus.get(term.getKey());
					finalLapScore = finalLapScore + Math.log((double)(1/(docLengthMap.get(docno)+V)));
					finalJMScore = finalJMScore + Math.log(((1-lambda)*tfdC/docLengthC));	
				}
			}
			
			finalLapScore = finalLapScore + laplaceScore;
			uniLaplace.put(docno, finalLapScore);
			
			finalJMScore = finalJMScore + jmScore;
			uniJM.put(docno, finalJMScore);
			
		}
		
		lmMaps.add(uniLaplace);
		lmMaps.add(uniJM);
		return lmMaps;
	}
	
	
}
