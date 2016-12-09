dataload = LOAD 'data/sites.txt'
    USING PigStorage('\t') 
    AS ( url: chararray, pagerank: float, links:{ link:tuple(url: chararray) } );

flattened_pagerank =FOREACH dataload 
    GENERATE 
        pagerank / COUNT ( links ) AS pagerank, FLATTEN ( links ) AS to_url;

grouped_pagerank = 
	FOREACH ( COGROUP flattened_pagerank BY to_url, dataload BY url INNER )
	GENERATE 
		group AS url, 
        	0.15+0.85*SUM(flattened_pagerank.pagerank) AS pagerank, 
        	FLATTEN ( dataload.links ) AS links;

pgrn_order = order grouped_pagerank by pagerank DESC;
  
STORE pgrn_order 
    INTO './result-pig' 
    USING PigStorage('\t');


