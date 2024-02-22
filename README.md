# Adobserve_whitelist
A ‘whitelist’ data gathering pipeline approach that gathers ads directly from platform-provided transparency libraries into aggregate collections for downstream analysis

## Environment requirements
- **HomeBrew** Package Management Command Line Interface to install, update and manage software on MacOS
- **Git** Version control system to collaborate as well as to clone the public repos or repos with at least reading access. 
- **Python** Python 3.8.5
- **Anaconda/VirtualEnv** To create an environment in which all the dependencies/libraries for this CLI tool are installed.
- **AWS**
- **GBQ**

## Setting up
- Create a Conda environment, remember the name of your environment to activate it every time you like to use this tool.
```
conda init
conda create --name <replace_this> python=3.8
conda activate <replace_this>   
```
- Clone the code from GitHub repository
```
git clone https://github.com/qut-dmrc/adobserve_whitelist.git
cd source
pip install -r requirements.txt
```

## Steps to start collecting a new dataset
  1. Get a list of FB pages to track
  2. There are two ways to gather FB page IDs.
     * Get IDs from CrowdTangle(CT) given a list of **FB Profile URLs** to public accounts.. <em>Access to CrowdTangle needs to be requested separately. </em> This method is more robust than getting IDs directly from Facebook Ad library, but you can skip this if you do not have access to CT.
       - a. Create a new empty list in one of the CT dashboards. E.g. whitelist_method_test
       - b. Run CT_page_id/clearn_url_to_CT_template.py and batch upload the template, and keep a record of pages that failed to be tracked.
         ```
         filename = 'Ad monitoring targets.csv' #The csv file with FB page URLs
         target_col = "Page ID" #The column with FB page URLs
         list_name = "whitelist_method_test"
         create_CT_list(filename,target_col,list_name)
         ```
       - c. Run CT_page_id/facebook_page_info_CT.py to get account info from CT (mainly for page name to be used on ad library to search, and accountHandle to verify it's the right account)
      * Get IDs from Meta Ad Library directly given a list of **account names**. (Step 3)
  3. Get page IDs from the Facebook ad library (FB_page_id/facebook_ad_library_page_id.py)
     * Replace credentials before running this script.
          1. Go to Meta Ad Library.
          2. Right click to inspect.
          3. Go to Network tab.
          4. Interact with the website and find the right server request that gives you the response which contains page info.
          5. Right click to copy as cURL.
          6. Conver cURL to Python request.
          7. Copy all credentials and paste them in get_page_info
          8. Replace `'q': '...'` in params with `'q' : page["name"]`
          9. Update variables to point to your CSV file, update column names for page name and handle.
          10. Run FB_page_id/facebook_ad_library_page_id.py
          11. You can get your ID from pages.csv, page info from results/[whitelist_test].json \[You can replace whitelist_test with a meaningful name for your case study.\]
  5. Run CT_page_id/merge_id_url.py. Output:
     - a. 1st iteration of page_url, name, id csv file, comment out #3 onward in the code
     - b. [do not edit in excel and save as csv, all ids will be truncated, edit it directly in code editor] cross-check with facebook ad library  or library( https://www.facebook.com/ads/library/report/?source=nav-header), may need to gather IDs manually for those that could not be retrieved programmatically. 
     - c. Once the IDs are complete, comment out #1 in the code. Run the second part(#3) of the script
     - d. Ids.txt contains all the IDs to be added to the tracking list.
     - e. Final excel/csv file with original data and id column are for reference. 
  6. Use the ids to collect ads from ad library. \[Check steps to collect ads from id regularly\]
  7. Json files in results can be uploaded to a structured database of choice \[Google Big Query in our example\] named as page data
      a. Run update_gbq_tables_page_info.py to gather info for ids that were added later
      b. Run update_gbq_tables.py
  8. Update page category data on GBQ too.
      a. update_page_category.py

### Steps to collect data regularly
1. Check config.py contains IDs to track assigned to variable pages.
   E.g. pages = ['123','456','789']
2.  Create folders media/ and raw_json/ inside source/
3.  Comment out `combine_new_data_with_existing(tablenames, dataset)` when you run it for the first time
4.  run `python __main__.py`
5. If the process gets interrupted midway through the list of pages, change the latest json file with today's date to the name of your dataset(e.g. whitelist_test) and set mainLoop in `if __name__ == '__main__'` to `mainLoop(dataset, False, True)`



## Contact Us
Please do not hesitate to contact our team for any inquiries or assistance.
<br />
Jane Tan x28.tan@qut.edu.au
<br />
Daniel Angus daniel.angus@qut.edu.au
