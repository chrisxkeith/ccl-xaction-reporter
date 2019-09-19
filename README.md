# ccl-xaction-reporter
_... in progress ..._

Python script to report delinquent member payments.

Steps to set up (one time):
1. Install python.
2. Download the script and associated files from https://github.com/chrisxkeith/ccl-xaction-reporter into a folder (“working folder”) on your computer where you will keep the downloaded data files.
3. Run a Terminal window. Instructions TBD.
4. Change directory into the folder -- terminal command TBD
5. Run the following command -- terminal command TBD

Monthly (or whatever your schedule is):
1. Download payment data from Stripe.
2. Make sure to overwrite any existing file. E.g., don’t create a file with a “(2)” or “copy” in the filename.
3. Move the file into the working folder.
4. Export the Google sheet (“Current Member List” tab only) containing the master membership list. (File -> Download -> .csv)
Overwrite any existing file named “CCL Members Master List - Current Member List.csv”
If overwriting isn’t possible, delete the old file and rename the new one.
5. Move the file into the working folder.
6. Run a Terminal window. instructions TBD
7. Change directory into the folder. command line TBD
8. Run the following command. command line TBD.
You should then have a file in that folder named <TBD>.csv
You can then File -> Import into a Google Sheet.

Notes:
- Does not handle dues paid through PayPal or cash. This functionality can be added when you have a way to download the PayPal transactions, and have provided an example of the file. For cash, you will have to manually create a .csv file of the cash transactions in .csv  format. It would be good to have this file in the same format as the Stripe data file.
