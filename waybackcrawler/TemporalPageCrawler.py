from Page import CdxPage, WayBackPage
from URLFilter import URLFilter
import threading
import Queue
import time

# SLEEP_TIME 
# Each query has some seconds of sleep so as to not overload the server
SLEEP_TIME = 2;

class TemporalPageCrawler:
    """
        Class represents a single class node crawl
        a) Given a URL it first gets all the CDX page and gets the timestamps of all images
        b) Then it goes through each time stamp and creates edges to all other nodes
    """
    def __init__(self, 
                 base_page_url, 
                 filter_obj = None, 
                 out_file = None):
        self.base_page_url = base_page_url;
        if filter_obj and not isinstance(filter_obj, URLFilter):
            raise Exception("Follwing filter is not supported");
        self.filter_obj = filter_obj;
        self.write_to_file = False;
        self.out_file = out_file;
        self.out_file_fh = None; 
        if out_file:
            self.write_to_file = True;
            self.out_file = out_file;
            self.out_file_fh = None; 
        return;

    def update_filter_function(self, filter_obj):
        if not isinstance(filter_obj, URLFilter):
            raise Exception("Follwing filter is not supported");
        self.filter_obj = filter_obj;

    def write_to_file(out_file):
        if self.write_file_fh:
            raise Exception("Old File is already being written");
        self.write_to_file = True;
        self.out_file = out_file;
        self.write_file_fh = None;

    def get_cdx_info(self):
        self.cdx_object = CdxPage(self.base_page_url);
        return self.cdx_object.get_timestamped_url();

    def get_urls_in_page_on(self, time_stamp, original_url):
        # Be good Stop for some time 
        time.sleep(SLEEP_TIME);
        page_obj = WayBackPage(time_stamp, original_url);
        url_list = page_obj.get_urls();
        if self.filter_obj:
            url_list = self.filter_obj.filter(url_list);
        return url_list;

    def serialize_to_file(self, timestamp_url_list):
        if not self.out_file_fh:
            self.out_file_fh = open(self.out_file, "a+");
        for timestamp, to_url in timestamp_url_list:
            self.out_file_fh.write("%s\t%s\t%s\n"%(self.base_page_url,
                                                     to_url,
                                                     timestamp));
    
    def get_temporal_crawl(self):
        timestamped_name_list = self.get_cdx_info();
        complete_timestamped_url = [];
        for entry_hash in timestamped_name_list:
            to_list = self.get_urls_in_page_on(entry_hash['timestamp'],
                                                 entry_hash['original_url']);
            timestamped_urls = [(entry_hash['timestamp'], to_url) for to_url in to_list];
            if self.write_to_file:
                self.serialize_to_file(timestamped_urls);
            else:
                complete_timestamped_url.extend(timestamped_urls);
        if self.write_to_file:
            self.out_file_fh.close();
            return None;
        return complete_timestamped_url;


class TemporalPageCrawlerThreaded(TemporalPageCrawler):
    def __init__(self, 
                 base_page_url, 
                 filter_obj = None, 
                 out_file_fn = None,
                 thread_count = 20):
        """
        thread_count - We create thread_count-1 URL query threads and on write thread. The reason for only one write thread
                       is since we will be serializing so that the result from one page are aggregated. There is no point 
                       having multiple write points as they will just be stuck
        """
        TemporalPageCrawler.__init__(self,
                                     base_page_url,
                                     filter_obj,
                                     out_file_fn);
        self.thread_count = thread_count;
        self.__create_threads();
        if not self.write_to_file:
            # Since in this case we need to aggregate the result
            self.complete_timestamped_urls = [];
        return;


    def __create_threads(self):
        self.url_thread_list = [threading.Thread(target = self.__get_urls_threaded, 
                                                  name  = "Url Thread "+ str(i)) for i in range(1, self.thread_count)];
        self.write_thread = threading.Thread(target = self.__serialize_to_file_threaded,
                                             name = "Writer Thread");
        self.url_job_queue = Queue.Queue();
        self.url_result_queue = Queue.Queue();


    def __start_threads(self):
        for thread in self.url_thread_list:
            thread.setDaemon(True);
            thread.start();
        self.write_thread.setDaemon(True);
        self.write_thread.start();

    def __stop_threads(self):
        for thread in self.url_thread_list:
            thread.stop();
        self.write_thread.stop();

    
    def __get_urls_threaded(self):
        while True:
            timestamped_urls = [];
            (time_stamp, original_url) = self.url_job_queue.get();
            try:
                to_list = self.get_urls_in_page_on(time_stamp,
                                                   original_url);
                timestamped_urls = [(time_stamp, to_url) for to_url in to_list];
                self.url_result_queue.put(timestamped_urls);
                self.url_job_queue.task_done();
            except Exception as e:
                # This is a troublesome case Just protecting from code hanging here
                print "Error for Url %s %s"%(time_stamp, original_url);
                print e;
                self.url_job_queue.task_done();


    def __serialize_to_file_threaded(self):
        while True:
            timestamped_urls = self.url_result_queue.get();
            if self.write_to_file:
                self.serialize_to_file(timestamped_urls);
            else:
                self.complete_timestamped_urls.extend(timestamped_urls);
            self.url_result_queue.task_done();
    

    def get_temporal_crawl(self):
        timestamped_name_list = self.get_cdx_info();
        # Now we start the threads
        self.__start_threads();

        for entry_hash in timestamped_name_list:
            self.url_job_queue.put((entry_hash['timestamp'],
                                    entry_hash['original_url']));
        # Now First we Wait for the job_queue to get empty
        self.url_job_queue.join();
        # Now we wait for the serializing queue to get empty
        self.url_result_queue.join();
        # We can stop the threads now
        if self.write_to_file:
            self.out_file_fh.close();
            return None;
        return self.complete_timestamped_urls;




if __name__ == "__main__":
    class TestFilter(URLFilter):
        def __init__(self):
            URLFilter.__init__(self);
            return;
        
        def filter(self, url_list):
            result_list = [];
            for url in url_list:
                if 'org' in url:
                    result_list.append(url);
            return result_list;

    print TemporalPageCrawlerThreaded("http://www.accesshollywood.com/",
                                      TestFilter(),
                                      "my_test.out").get_temporal_crawl();
