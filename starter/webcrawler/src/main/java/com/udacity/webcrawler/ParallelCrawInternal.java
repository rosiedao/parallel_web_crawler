package com.udacity.webcrawler;

import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.RecursiveTask;
import java.util.regex.Pattern;

public final class ParallelCrawInternal extends RecursiveTask<Boolean> {
    private final String url;
    private final Instant deadline;
    private final int maxDepth;
    private final ConcurrentHashMap<String, Integer> counts;
    private final ConcurrentSkipListSet<String> visitedUrls;
    private final Clock clock;
    private final PageParserFactory pageParserFactory;
    private final List<Pattern> ignoredUrls;

    public ParallelCrawInternal(String url, Instant deadline, int maxDepth, ConcurrentHashMap<String, Integer> counts,ConcurrentSkipListSet<String> visitedUrls, Clock clock,
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
        //Synchronized block

        synchronized (this) {
            try {
                if (visitedUrls.contains(url)) {
                return false;
                }visitedUrls.add(url);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        PageParser.Result result = pageParserFactory.get(url).parse();
        for (Map.Entry<String, Integer> e : result.getWordCounts().entrySet()) {
            counts.compute(e.getKey(),(word,number) -> (number == null) ? e.getValue() : e.getValue() + number);
        }

        List<ParallelCrawInternal> parallelTasks = new ArrayList<>();
        for (String link : result.getLinks()) {
            parallelTasks.add(new ParallelCrawInternal(link,deadline,maxDepth-1,counts,visitedUrls,clock,pageParserFactory,ignoredUrls));
        }
        invokeAll(parallelTasks);
        return true;
    }
}
