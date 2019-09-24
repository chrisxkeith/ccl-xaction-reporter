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
    gsheets_fieldnames = []
    gsheets_dict_records = {}
    paypal_to_membership_email_mapping = {'rolfvw@pizzicato.com' : 'rolfvw@gmail.com', 'alan@halo.nu' : 'alanrockefeller@gmail.com'}
    # cancelled membership: 'matthew.stewart.mi@gmail.com' : 'codehesionoakland@gmail.com'

    def to_std_date_fmt(self, date_str):
        mon, day, year = date_str.split('/')
        dt = datetime.datetime(int('20' + year), int(mon), int(day), 0, 0, 0)
        return dt.strftime('%Y/%m/%d')

    def stripe_date(self, datetime_str):
        date_str, time_str = datetime_str.split(' ')
        return self.to_std_date_fmt(date_str)

    def find_latest_record(self, dict, record, email, date_col_name):
        # - compare timestamps, keep latest payment record.
        if dict.get(email.strip()):
            current_date = dict[email.strip()][date_col_name]
            new_date = record[date_col_name]
            if new_date > current_date:
                dict[email.strip()] = record
        else:
            dict[email.strip()] = record

    def handle_stripe(self, dict, record):
        junk, email = record['Customer Description'].split('|')
        record['Created (UTC)'] = self.stripe_date(record['Created (UTC)'])
        self.find_latest_record(dict, record, email, 'Created (UTC)')

    def handle_paypal(self, dict, record):
        if 'Credit' == record['Balance Impact']:
            if self.paypal_to_membership_email_mapping.get(record['From Email Address']):
                record['From Email Address'] = self.paypal_to_membership_email_mapping.get(record['From Email Address'])
            # Test Type and Note columns to detect if this is a membership dues payment.
            if 'ubscription' in record['Type'] or 'ember' in record['Note'] or 'ues' in record['Note'] \
                    or self.gsheets_dict_records.get(record['From Email Address']):
                record['Date'] = self.to_std_date_fmt(record['Date'])
                self.find_latest_record(dict, record, record['From Email Address'], 'Date')

    def handle_members(self, dict, record):
        record['Email'] = record['Email'].strip()
        record['Days Delinquent'] = ' '
        record['Last Payment Date'] = ' '
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

    def find_latest_payment(self, gsheets_rec, date_str, should_be_paid_by_date):
        gsheets_rec['Last Payment Date'] = date_str
        last_paid_date = datetime.datetime.strptime(date_str, '%Y/%m/%d')
        if last_paid_date < should_be_paid_by_date:
            tdiff = should_be_paid_by_date - last_paid_date
            gsheets_rec['Days Delinquent'] = str(tdiff.days)

    def merge_payment_dates(self, stripe_dict_records, paypal_dict_records):
        should_be_paid_by_date = datetime.datetime.now().replace(day=10)
        for r in self.gsheets_dict_records.keys():
            gsheets_rec = self.gsheets_dict_records.get(r)
            gsheets_rec['Payment Method'] = 'n/a'
            if gsheets_rec.get('Payment Amount') and int(gsheets_rec['Payment Amount']) > 0:
                gsheets_rec['Payment Method'] = 'unknown'
                if gsheets_rec['Email'] in ['litchfield.ken@gmail.com', 'thalula@peralta.edu', 'natarajn@aol.com', 'nenufarmoleculesforlife@gmail.com']:
                    gsheets_rec['Payment Method'] = 'cash'
                else:
                    if stripe_dict_records.get(r):
                        gsheets_rec['Payment Method'] = 'Stripe'
                        date_str = stripe_dict_records.get(r).get('Created (UTC)')
                        self.find_latest_payment(gsheets_rec, date_str, should_be_paid_by_date)
                    else:
                        if paypal_dict_records.get(r):
                            gsheets_rec['Payment Method'] = 'PayPal'
                            date_str = paypal_dict_records.get(r).get('Date')
                            self.find_latest_payment(gsheets_rec, date_str, should_be_paid_by_date)

    def get_record_key(self, array_record):
        lpd = ''
        if len(array_record) > self.field_indices['Last Payment Date'] and array_record[self.field_indices['Last Payment Date']]:
            lpd = array_record[self.field_indices['Last Payment Date']]
        return array_record[self.field_indices['Status']] + ' ' + lpd

    def write_payment_statuses(self):
        array_records = []
        for r in self.gsheets_dict_records.values():
            array_records.append(self.to_array(r))
        sorted_by_value = sorted(array_records, key=self.get_record_key)
        self.gsheets_fieldnames.append('Last Payment Date')
        self.gsheets_fieldnames.append('Days Delinquent')
        self.gsheets_fieldnames.append('Payment Method')
        out_file_name = 'payment_statuses.csv'
        with open(out_file_name, 'w', newline='') as outfile:
            writer = csv.DictWriter(outfile, self.gsheets_fieldnames, delimiter=',', quotechar='"',
                                    quoting=csv.QUOTE_MINIMAL)
            writer.writeheader()
            c = 0
            for record in sorted_by_value:
                the_dict = self.to_dict(record)
                writer.writerow(the_dict)
                c += 1
        log(str("{: >4d}".format(c)) + ' records written to ' + out_file_name)

    def write_full_email_list(self, stripe_dict_records):
        master_list = {}
        for k in stripe_dict_records.keys():
            master_list[k] = {'Email' : k, 'In Stripe' : 'Y', 'In Google Sheet' : '',
                'Stripe payment date' : stripe_dict_records[k]['Created (UTC)'],
                'Notes' : ''}
        for k in self.gsheets_dict_records.keys():
            if master_list.get(k):
                master_list.get(k)['In Google Sheet'] = 'Y'
                master_list.get(k)['Notes'] = self.gsheets_dict_records[k]['Notes']
            else:
                master_list[k] = {'Email' : k, 'In Stripe' : '', 'In Google Sheet' : 'Y',
                    'Stripe payment date' : '', 'Notes' : self.gsheets_dict_records[k]['Notes'] }
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
        self.gsheets_fieldnames, self.gsheets_dict_records = self.read_from_stream_into_dict(
            'Member list for export for python report - Sheet1.csv',
            self.handle_members)
        stripe_fieldnames, stripe_dict_records = self.read_from_stream_into_dict(
            'STRIPE_unified_payments.csv',
            self.handle_stripe)
        paypal_fieldnames, paypal_dict_records = self.read_from_stream_into_dict(
            'PayPalData2019.csv',
            self.handle_paypal)
        i = 0
        for field_name in self.gsheets_fieldnames:
            self.field_indices[field_name] = i
            self.field_names_dict[i] = field_name
            i += 1
        self.field_indices['Last Payment Date'] = i        
        self.field_names_dict[i] = 'Last Payment Date'
        i += 1
        self.field_indices['Days Delinquent'] = i        
        self.field_names_dict[i] = 'Days Delinquent'
        i += 1
        self.field_indices['Payment Method'] = i       
        self.field_names_dict[i] = 'Payment Method'
        self.merge_payment_dates(stripe_dict_records, paypal_dict_records)
        self.write_payment_statuses()
        self.write_full_email_list(stripe_dict_records)

if '__main__' == __name__:
    Reporter().main()
