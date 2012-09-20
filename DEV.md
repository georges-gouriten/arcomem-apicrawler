# Development background

Making the API crawler has been a long process with many evolutions so,
sorry about that, don't expect the best code ever.

Moreover, the project was not started with a TDD approach and I was a bit
short on time to implement all the tests I would have liked to implement.
Currently, the tests are only on the vital functions and what works now
could fail with weird input data for instance.

However, I tried to clarify the code and make it as clear as possible and
it currently passes all the tests and works (inputs are properly processed,
outputs are good).

Basically, what the crawler does is:

*   it starts threads for each platform crawled and the different output
module (triples, warcs, outlinks),
*   it receives HTTP request, handled by the apicrawler module (that uses
web.py), then instructions are passed to the interface module,
*   the interface module takes care of creating the crawl, 
*   the crawl creates the spiders; a crawl can have several spiders, for
instance, a crawl repeating every six hours during 24 hours will have 4
spiders,
*   the spiders are added to the different platforms' queues
*   the platforms starts the spiders at the convenient time (after
start_date),
*   the data produced is sent to the different output modules,
*   the WARC module writes down the whole responses into WARC files,
*   the outlinks module extracts outlinks from the responses and sends it
the heritrix crawler or writes it down into a backup file,
*   the triples module makes triples and sends it to the triple store or
writes it down into a backup file and,
*   it logs everything into the different log files.

# Code architecture

Please refer to the explanations above, to the doc folder and to the source code.

# Next steps
 
There are a three important TODO left, you can grep 'TODO' to find them:

*   add the communication with the IMF crawler in the outlinks module,
*   add the put_triples method to write the triples down in the triple
    store (the method is not ready yet as of 09/20/12)
*   add the get_apicrawler_ics method to get the crawl spec from the triple
    store (the method is not ready as of 09/20/12) 

I also added some ideas to improve the software, you can grep 'IDEA' to
find them.

Feel free to contact me if you have any question and good luck!

Georges
