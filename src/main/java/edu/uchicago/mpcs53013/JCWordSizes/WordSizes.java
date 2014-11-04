package edu.uchicago.mpcs53013.JCWordSizes;

import java.util.Arrays;
import java.util.List;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;

import clojure.lang.PersistentVector;

import com.twitter.maple.tap.StdoutTap;

import jcascalog.Api;
import jcascalog.Playground;
import jcascalog.Subquery;
import jcascalog.op.Count;

public class WordSizes {
	public static class Split extends CascalogFunction {
		public void operate(FlowProcess process, FunctionCall call) {
			String sentence = call.getArguments().getString(0);
			for (String word: sentence.split(" ")) {
				int length = word.length();
				call.getOutputCollector().add(new Tuple(length));
			}
		}
	}
	PersistentVector v;
	static List SENTENCE= PersistentVector.create(Arrays.asList(
			PersistentVector.create(Arrays.asList("Four score and seven years ago our fathers")),
			PersistentVector.create(Arrays.asList("brought forth on this continent a new nation")),
			PersistentVector.create(Arrays.asList("conceived in Liberty and dedicated  to")),
			PersistentVector.create(Arrays.asList("the proposition that all men are created equal"))));
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Api.execute(new StdoutTap(),
				new Subquery("?word", "?count")
		.predicate(Api.hfsTextline("hdfs://localhost:54310/tmp/odyssey.txt"), "?sentence")
		.predicate(new Split(), "?sentence").out("?word")
		.predicate(new Count(), "?count"));
	}

}