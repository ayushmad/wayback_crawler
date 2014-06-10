from waybackcrawler import TemporalPageCrawler, URLFilter
import csv
import sys
import os
import urlparse
import threading
import Queue
import time


# This is a Example Use Case of the Package
# The Script takes a list of nodes as a input it 
# Then generates a list of edges for each URL on the specific timestamps
# This is collected for all graphs

class GraphURLFilter(TemporalPageCrawler.URLFilter):
    """
        We need to filter and select only those URLS
        which are provided in our set. We domain hashing for our task
        Basically given a url, we extract the netloc and hash it.
        Even though this would allow a significant number of more urls, than exact match
        But we are fine with them.
    """
    def __init__(self, allowed_urls):
        TemporalPageCrawler.URLFilter.__init__(self);
        self.allowed_hostnames = self.__create_host_name_hash(allowed_urls);

    def __create_host_name_hash(self, allowed_urls):
        allowed_hostnames = {};
        for url in allowed_urls:
            hostname = self.__get_hostname(url);
            if hostname != None:
                allowed_hostnames[hostname] = True;
        return allowed_hostnames;

    def __get_hostname(self, url):
        hostname = urlparse.urlparse(url).hostname;
        if not hostname:
            # This could happen because of two issues 
            # a) Url has our needed link inside
            # b) Url does not start with a schema
            # Case a) 
            schema_loc = url.find("http");
            if schema_loc != -1:
                sub_url = url[schema_loc:];
                hostname = urlparse.urlparse(sub_url).hostname;
                return hostname;
            protocol_loc = url.find('www');
            if protocol_loc != -1:
                sub_url = "http://" + url[protocol_loc:];
                hostname = urlparse.urlparse(sub_url).hostname;
                return hostname;
            # Case b)
            sub_url = "http://" + url;
            hostname = urlparse.urlparse(sub_url).hostname;
        return hostname

    def filter(self, url_list):
        filtered_urls = [];
        for url in url_list:
            hostname = self.__get_hostname(url);
            if self.allowed_hostnames.has_key(hostname):
                filtered_urls.append(url);
        return filtered_urls;


class GraphURLFilterSingleton(GraphURLFilter):
    """
        This is a extension of the GRaph url filter which
        only allows one url. Hence If the url crawl is completed we remove it
    """
    def __init__(self, allowed_urls):
        GraphURLFilter.__init__(self);
        self.allowed_hostnames = self.__create_host_name_hash(allowed_urls);

    def filter(self, url_list):
        filtered_urls = [];
        for url in url_list:
            hostname = self.__get_hostname(url);
            if self.allowed_hostnames.has_key(hostname) and \
                        self.allowed_hostnames[hostname]:
                self.allowed_hostnames[hostname] = False
                filtered_urls.append(url);
        return filtered_urls;


class GraphCrawler:
    """
        Grap
    """
    def __init__(self, input_file_name, file_delimiter = "\t"):
        self.file_delimiter = file_delimiter;
        self.input_hash = self.process_csv_file(input_file_name,
                                                file_delimiter);
        self.output_file_name = self.__make_output_file_name(
                                        input_file_name);

        # Setting up progress bar functionality 
        self.progress_queue = Queue.Queue();
        self.progress_bar_thread = threading.Thread(target = self.progress_bar);
        self.stopreuest = threading.Event();
        self.refreash_rate = 60;

    def __make_output_file_name(self, input_file_name):
        fileName, fileExtension = os.path.splitext(input_file_name);
        return fileName + ".out";

    def process_csv_file(self, in_file_name, delimiter):
        result_hash = [];
        with open(in_file_name, 'rb') as csv_file:
            inp_stream = csv.DictReader(csv_file, 
                                        delimiter = delimiter);
            for entry in inp_stream:
                result_hash.append(entry);
        return result_hash;

    def progress_bar(self):
        print "Starting Collection\n";
        message_printed = False;
        prefix = "Completed";
        suffix = "Node Crawls";
        completed_node_count = 0;
        while not self.stopreuest.isSet():
            if not self.progress_queue.empty():
                completed_node_count = self.progress_queue.get();
                while not self.progress_queue.empty():
                    completed_node_count = self.progress_queue.get();
                if not message_printed:
                    print "%s %5d %s"%(prefix,
                                        completed_node_count,
                                        suffix),
                    # This is because the crawl count 
                    # is given 10 positions and we have a space
                    print "\b"*(8 + len(suffix)),
                    sys.stdout.flush();
                    message_printed = True;
                else:
                    print "%5d"%(completed_node_count),
                    print "\b"*7,
                    sys.stdout.flush();
            time.sleep(self.refreash_rate);
        print "%6d %s\nGraph Collected at: %s"%(completed_node_count,
                                               suffix, 
                                               self.output_file_name), 
        sys.stdout.flush();

    def crawl(self):
        my_filter = GraphURLFilter(
                    [entry['URL'].strip() for entry in self.input_hash]);
        node_count = 0;
        self.progress_queue.put(node_count);
        self.progress_bar_thread.start();
        for entry in self.input_hash:
            TemporalPageCrawler.TemporalPageCrawlerThreaded(
            entry['URL'].strip(),
            my_filter,
            self.output_file_name).get_temporal_crawl();
            node_count += 1;
            self.progress_queue.put(node_count);
        # Job Completed Close the progress bar gracefully
        self.stopreuest.set();
         # waiting for progress bar to wake up and print the close message
        self.progress_bar_thread.join(self.refreash_rate+10);
        if self.progress_bar_thread.isAlive():
            # Trouble in paradise
            self.progress_bar_thread.stop();
        


if __name__ == "__main__":
    # Crawling over test set
    GraphCrawler(sys.argv[1]).crawl();
