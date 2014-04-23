import urllib
import urllib2
from bs4 import BeautifulSoup

class Page:
    """
        Represents a simple accessible page
        Page initiates gets content in a lazy way 
        and then caches the response.
        Methods :-
        get_content - returns the content of the page
        get_urls - extracts and returns all the urls in the page
    """
    def __init__(self, url):
        self.url = url;
        self.response_content = None;
    
    def get_content(self):
        if not self.response_content:
            self.response_content = urllib2.urlopen(self.url).read();
        return self.response_content;

    def get_urls(self):
        url_list = [];
        result = self.get_content();
        my_soup = BeautifulSoup(result);
        for a in my_soup.find_all('a', href=True):
            url_list.append(a['href']);
        return url_list;


class CdxPage(Page):
    def __init__(self, url, limit = 1000):
        self.limit = limit;
        self.resume_key = False;
        self.target_url = url;
        url = self.__create_url();
        Page.__init__(self, url);

    def __create_url(self):
        base_url =  "http://web.archive.org/cdx/search/cdx?";
        query_params = {'url' : self.target_url,
                        'limit': self.limit,
                        'showResumeKey': 'true'};
        if (self.resume_key):
            query_params['resumeKey'] =  self.resume_key;
        return base_url + urllib.urlencode(query_params);

    def __reset_page_url(self, url):
        self.url = url;
        # Since the page url is updated we need to reset get content
        self.response_content = None;

    def __extract_cdx_data(self, data):
        result_list = []
        key_flag = False;
        for line in data.split('\n'):
            line = line.strip();
            if key_flag:
                self.resume_key = line.strip();
                break;
            if not line: # Blank line means key should be next
                key_flag = True;
                continue;
            line_parts = line.split();
            result_list.append({'timestamp': line_parts[1],
                                'original_url': line_parts[2]});
        return result_list

    def get_timestamped_url(self):
        timestamp_list = []
        while True:
            result_content = self.get_content();
            current_result = self.__extract_cdx_data(result_content);
            timestamp_list.extend(current_result);
            # Checking if any result returned
            if len(current_result) == self.limit:
                # Since there may be some entries left we may need to query again
                self.__reset_page_url(self.__create_url());
            else:
                break;
        return timestamp_list;


class WayBackPage(Page):
    def __init__(self, time_stamp, original_url):
        base_url =  "http://wayback.archive.org/web" ; # Cdx archive is accessible a get request on this url
        self.url = base_url + '/' + time_stamp + '/' + original_url;
        Page.__init__(self, self.url);
    
if __name__ == "__main__":
    cdx_obj = CdxPage("http://socialitelife.buzznet.com/");
    time_stamp_list = cdx_obj.get_timestamped_url();
    print time_stamp_list;
    print WayBackPage(time_stamp_list[0]['timestamp'], time_stamp_list[0]['original_url']).get_urls();
