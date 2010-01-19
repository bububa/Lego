= About Lego =
    Lego is an advance web crawler library written in python. It
provides a number of methods to mine data from kinds of sites. You 
could use YAML to create crawler templates don't need to know write 
python code for a web crawl job.
This project is based on SuperMario (http://github.com/bububa/SuperMario/).

== License ==
BSD License
See 'LICENSE' for details.

== Requirements ==
Platform: *nix like system (Unix, Linux, Mac OS X, etc.)
Python: 2.5+
Storage: mongodb
Some other python models:
    - bububa.SuperMario

== Features ==
  + YAML style templates;
  + keywords IDF calculation
  + keywords COEF calculation
  + Sitemanager
  + Smart Crawling
  + 

== DEMO ==
Sample YAML file to crawl snipplr.com

#snipplr.yaml
run: !SiteCrawler
    check: !Crawlable
        yaml_file: sites.yaml
        label: snipplr
        logger:
            filename: snipplr.log
    crawler: !YAMLStorage
        yaml_file: sites.yaml
        label: snipplr
        method: update_config
        data: !DetailCrawler
            sleep: 3
            proxies: !File
                method: readlines
                filename: /proxies
            pages: !PaginateCrawler
                proxies: !File
                    method: readlines
                    filename: /proxies
                url_pattern: http://snipplr.com/all/page/{NUMBER}
                start_no: 0
                end_no: 0
                multithread: True
                wrapper: '<ol class="snippets marg">([^^].*?)</ol>'
                logger:
                    filename: snipplr.log
            url_pattern: '/view/\d+/[^^]*?/$'
            wrapper:
                title: '<h1>([^^]*?)</h1>'
                language: '<p class="nomarg"><span class="rgt">Published in: ([^^]*?)</span>'
                author: '<h2>Posted By</h2>\s+<p><a[^^]*?>([^^]*?)</a>'
                code: '<a rel="nofollow" href="/view.php\?codeview&amp;id=(\d+)">'
                comment: '<div class="description">([^^]*?)</div>'
                tag: '<h2>Tagged</h2>\s+<p>([^^]*?)</p>'
            essential_fields:
                - title
                - language
                - code
            multithread: True
            remove_external_duplicate: True
            logger:
                filename: snipplr.log
            page_callback: !Document
                label: snipplr
                method: write
                page: None
                logger:
                    filename: snipplr.log
            furthure: 
                tag: 
                    parser: !Steps
                        inputs: None
                        steps: 
                            - !Dict
                                dictionary: None
                                method: member
                                args: 
                                    - tag
                            - !Regx
                                string: None
                                pattern: <a[^^]*?>([^^]*?)</a>
                                multiple: True
                code: 
                    parser: !Steps
                        inputs: None
                        steps: 
                            - !Dict
                                dictionary: None
                                method: member
                                args: 
                                    - code
                            - !String
                                args: None
                                base_str: 'http://snipplr.com/view.php?codeview&id=%s'
                            - !Init 
                                inputs: None 
                                obj: !URLCrawler
                                    urls: None
                                params: 
                                    save_output: True
                                    wrapper: '<textarea[^^]*?class="copysource">([^^]*?)</textarea>'
                            - !Array
                                arr: None
                                method: member
                                args: 
                                    - 0
                            - !Dict
                                dictionary: None
                                method: member
                                args: 
                                    - wrapper

-------------------------------------------------------------------
#sites.yaml
snipplr: 
    duration: 1800
    end_no: 1
    last_updated_at: 1263883143.0
    start_no: 1
    step: 1

--------------------------------------------------------------------

Run the crawler in shell

python lego.py -c snipplr.yaml

The results are stored in mongodb. You could use inserter Module to insert the data into mysql database.



  