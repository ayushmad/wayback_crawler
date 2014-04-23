Wayback Crawler
===============

This package crawls and create a graphs of webpages, over a time.
The package use wayback machine to get the older snapshots of the page.


Dependencies
------------
[Internet Archive CDX API][dl]
[dl]: https://github.com/internetarchive/wayback/blob/master/wayback-cdx-server/README.md
[LXML][]
[LXML]:http://lxml.de/index.html#documentation


Classes
=======
Page
----
Page class provides a wrapper around urllib open

WayBackPage
-----------
Page class for a wayback back page based on timestamp and url

CdxPage
-------
Page class which interacts with the CDX API's

URLFilter
---------
URL provides methods for filtering and selecting specific API's

TemporalPageCrawler
-------------------
Extracts all the urls from different timestamps of a given page

TemporalPageCrawlerThreaded
---------------------------
Threaded implementation of TemporalPageCrawler. Uses default of 20 threads.

TODO
====
a) Write Complete Documentation
b) Profile code and remove the bottleneck of Beautiful soup
c) Add Test code
