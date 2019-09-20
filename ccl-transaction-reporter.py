# Please credit chris.keith@gmail.com

import sys
import csv
import datetime

def log(message):
    script_name = sys.argv[0]
    print(str(datetime.datetime.now()) + '\t'+ script_name + ': ' + message)

class Reporter:
    field_indices = {}
    field_names_dict = {}

    def stripe_date(self, datetime_str):
        date_str, time_str = datetime_str.split(' ')
        mon, day, year = date_str.split('/')
        dt = datetime.datetime(int('20' + year), int(mon), int(day), 0, 0, 0)
        return dt.strftime('%Y/%m/%d')

    def handle_stripe(self, dict, record):
        junk, email = record['Customer Description'].split('|')
        record['Created (UTC)'] = self.stripe_date(record['Created (UTC)'])
        # - compare timestamps, keep latest payment record.
        if dict.get(email.strip()):
            current_date = dict[email.strip()]['Created (UTC)']
            new_date = record['Created (UTC)']
            if new_date > current_date:
                dict[email.strip()] = record
        else:
            dict[email.strip()] = record

    def handle_members(self, dict, record):
        record['Email'] = record['Email'].strip()
        record['Days Delinquent'] = ''
        dict[record['Email']] = record

    def read_from_stream_into_dict(self, file_name, dict_processing_funct):
        dict = {}
        fieldnames = None
        with open(file_name, 'r', newline='') as infile:
            reader = csv.DictReader(infile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            fieldnames = reader.fieldnames
            for record in reader:
                dict_processing_funct(dict, record)
        log(str("{: >4d}".format(len(dict))) + ' records read from ' + file_name)
        return fieldnames, dict 

    def to_array(self, dict_record):
        arr = []
        for key, value in dict_record.items():
            arr.insert(self.field_indices[key], value)
        return arr

    def to_dict(self, array_record):
        dict_record = {}
        i = 0
        for v in array_record:
            dict_record[self.field_names_dict[i]] = v
            i += 1
        return dict_record

    def merge_payment_dates(self, stripe_dict_records, gsheets_dict_records):
        should_be_paid_by_date = datetime.datetime.now().replace(day=10)
        for r in gsheets_dict_records.keys():
            if stripe_dict_records.get(r):
                date_str = stripe_dict_records.get(r).get('Created (UTC)')
                gsheets_dict_records.get(r)['Last Payment Date'] = date_str
                last_paid_date = datetime.datetime.strptime(date_str, '%Y/%m/%d')
                if last_paid_date < should_be_paid_by_date:
                    tdiff = should_be_paid_by_date - last_paid_date    
                    gsheets_dict_records.get(r)['Days Delinquent'] = str(tdiff.days)

    def get_record_key(self, array_record):
        lpd = ''
        if len(array_record) > self.field_indices['Last Payment Date'] and array_record[self.field_indices['Last Payment Date']]:
            lpd = array_record[self.field_indices['Last Payment Date']]
        return array_record[self.field_indices['Status']] + ' ' + lpd

    def write_payment_statuses(self, gsheets_fieldnames, gsheets_dict_records):
        array_records = []
        for r in gsheets_dict_records.values():
            array_records.append(self.to_array(r))
        sorted_by_value = sorted(array_records, key=self.get_record_key)
        gsheets_fieldnames.append('Last Payment Date')
        gsheets_fieldnames.append('Days Delinquent')
        out_file_name = 'payment_statuses.csv'
        with open(out_file_name, 'w', newline='') as outfile:
            writer = csv.DictWriter(outfile, gsheets_fieldnames, delimiter=',', quotechar='"',
                                    quoting=csv.QUOTE_MINIMAL)
            writer.writeheader()
            c = 0
            for record in sorted_by_value:
                if record[self.field_indices['Status']] == "Active":
                    the_dict = self.to_dict(record)
                    if the_dict.get("Last Payment Date"):
                        writer.writerow(the_dict)
                        c += 1
        log(str("{: >4d}".format(c)) + ' records written to ' + out_file_name)

    def write_full_email_list(self, stripe_dict_records, gsheets_dict_records):
        master_list = {}
        for k in stripe_dict_records.keys():
            master_list[k] = {'Email' : k, 'In Stripe' : 'Y', 'In Google Sheet' : '',
                'Stripe payment date' : stripe_dict_records[k]['Created (UTC)'],
                'Notes' : ''}
        for k in gsheets_dict_records.keys():
            if master_list.get(k):
                master_list.get(k)['In Google Sheet'] = 'Y'
                master_list.get(k)['Notes'] = gsheets_dict_records[k]['Notes']
            else:
                master_list[k] = {'Email' : k, 'In Stripe' : '', 'In Google Sheet' : 'Y',
                    'Stripe payment date' : '', 'Notes' : gsheets_dict_records[k]['Notes'] }
        field_names = ['Email', 'In Stripe', 'In Google Sheet', 'Stripe payment date', 'Notes']
        field_indices = {'Email' : 1, "In Stripe": 2, 'In Google Sheet' : 3,
            'Stripe payment date' : 4, 'Notes' : 5}
        out_file_name = 'full_email_list.csv'
        with open(out_file_name, 'w', newline='') as outfile:
            writer = csv.DictWriter(outfile, field_names, delimiter=',', quotechar='"',
                                    quoting=csv.QUOTE_MINIMAL)
            writer.writeheader()
            for record in master_list.items():
                writer.writerow(record[1])
        log(str("{: >4d}".format(len(master_list))) + ' records written to ' + out_file_name)

    def main(self):
        stripe_fieldnames, stripe_dict_records = self.read_from_stream_into_dict(
            'STRIPE_unified_payments.csv', self.handle_stripe)
        gsheets_fieldnames, gsheets_dict_records = self.read_from_stream_into_dict(
            'Member list for export for python report - Sheet1.csv', self.handle_members)
        i = 0
        for field_name in gsheets_fieldnames:
            self.field_indices[field_name] = i
            self.field_names_dict[i] = field_name
            i += 1
        self.field_indices['Last Payment Date'] = i        
        self.field_names_dict[i] = 'Last Payment Date'        
        self.field_indices['Days Delinquent'] = i + 1        
        self.field_names_dict[i + 1] = 'Days Delinquent'
        self.merge_payment_dates(stripe_dict_records, gsheets_dict_records)
        self.write_payment_statuses(gsheets_fieldnames, gsheets_dict_records)
        self.write_full_email_list(stripe_dict_records, gsheets_dict_records)

if '__main__' == __name__:
    Reporter().main()
