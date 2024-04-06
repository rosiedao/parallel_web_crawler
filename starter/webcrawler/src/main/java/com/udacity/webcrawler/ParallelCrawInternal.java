package com.udacity.webcrawler;

import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.RecursiveTask;
import java.util.regex.Pattern;

public class ParallelCrawInternal extends RecursiveTask<Boolean> {
    private String url;
    private Instant deadline;
    private int maxDepth;
    private Map<String, Integer> counts;
    private Set<String> visitedUrls;
    private Clock clock;
    private PageParserFactory pageParserFactory;
    private List<Pattern> ignoredUrls;

    public ParallelCrawInternal(String url, Instant deadline, int maxDepth, Map<String, Integer> counts,Set<String> visitedUrls, Clock clock,
                                PageParserFactory pageParserFactory, List<Pattern> ignoredUrls){
        this.url = url;
        this.deadline = deadline;
        this.maxDepth = maxDepth;
        this.counts = counts;
        this.visitedUrls = visitedUrls;
        this.clock = clock;
        this.pageParserFactory = pageParserFactory;
        this.ignoredUrls = ignoredUrls;
    }
    @Override
    protected Boolean compute() {
        if (maxDepth == 0 || clock.instant().isAfter(deadline)) {
            return false;
        }
        for (Pattern pattern : ignoredUrls) {
            if (pattern.matcher(url).matches()) {
                return false;
            }
        }
        if (visitedUrls.contains(url)) {
            return false;
        }
        visitedUrls.add(url);
        PageParser.Result result = pageParserFactory.get(url).parse();
        for (Map.Entry<String, Integer> e : result.getWordCounts().entrySet()) {
            counts.compute(e.getKey(),(key,value) -> (key == null) ? e.getValue() : e.getValue() + value);
        }

        List<ParallelCrawInternal> subtasks = new ArrayList<>();
        for (String link : result.getLinks()) {
            subtasks.add(new ParallelCrawInternal(link,deadline,maxDepth-1,counts,visitedUrls,clock,pageParserFactory,ignoredUrls));
        }
        invokeAll(subtasks);
        return true;
    }
}
