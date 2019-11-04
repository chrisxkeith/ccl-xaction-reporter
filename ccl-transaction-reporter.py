# Please credit chris.keith@gmail.com

import sys
import csv
import datetime
import os.path
from os import path

def log(message):
    script_name = sys.argv[0]
    print(str(datetime.datetime.now()) + '\t'+ script_name + ': ' + message)

class Reporter:
    field_indices = {}
    gsheets_fieldnames = []
    gsheets_dict_records = {}
    paypal_to_membership_email_mapping = {'rolfvw@pizzicato.com' : 'rolfvw@gmail.com', 'alan@halo.nu' : 'alanrockefeller@gmail.com'}

    def stripe_date(self, datetime_str):
        date_str, time_str = datetime_str.split(' ')
        year, month, day = date_str.split('-')
        dt = datetime.datetime(int(year), int(month), int(day), 0, 0, 0)
        return dt.strftime('%Y/%m/%d')

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
        if '|' in record['Customer Description']:
            junk, email = record['Customer Description'].split('|')
            record['Created (UTC)'] = self.stripe_date(record['Created (UTC)'])
            self.find_latest_record(dict, record, email, 'Created (UTC)')

    def found_dues_note(self, record):
        if self.gsheets_dict_records.get(record['From Email Address']):
            return True
        # Test text in certain columns to detect if this is a membership dues payment.
        for type in ['Type', 'Note', 'Description']:
            if record.get(type):
                return 'ubscription' in record[type] or 'ember' in record[type] or 'ues' in record[type]

    def convert_to_paypal(self, dict, record):
        if self.found_dues_note(record):
            mon, day, year = record['Date'].split('/')
            year_int = int(year)
            # Why can't PayPal provide 4-digit years? :(
            if year_int < 2000:
                year_int += 2000
            dt = datetime.datetime(year_int, int(mon), int(day), 0, 0, 0)
            record['Date'] =  dt.strftime('%Y/%m/%d')
            self.find_latest_record(dict, record, record['From Email Address'], 'Date')

    def handle_paypal(self, dict, record):
        if self.paypal_to_membership_email_mapping.get(record['From Email Address']):
            record['From Email Address'] = self.paypal_to_membership_email_mapping.get(record['From Email Address'])
        if record.get('Balance Impact'):
            if 'Credit' == record['Balance Impact']:
                self.convert_to_paypal(dict, record)
        else:
            if float(record['Net']) > 0:
                self.convert_to_paypal(dict, record)

    def get_delinquent_column_header(self):
        return 'Months Delinquent'

    def handle_members(self, dict, record):
        for n in ['Expected Payment Amount',
                'Last Payment Date',
                'Last Payment Amount',
                self.get_delinquent_column_header(),
                'Payment Method',
            ]:
            record[n] = ''
        record['Email'] = record['Email'].strip()
        dict[record['Email']] = record

    def handle_new_sheet(self, dict, record):
        dict[record['Email']] = record

    def read_from_stream_into_dict(self, file_name, dict_processing_funct):
        dict = {}
        fieldnames = None
        with open(file_name, 'r', newline='') as infile:
            reader = csv.DictReader(infile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            fieldnames = reader.fieldnames
            c = 1
            record = {}
            try:
                for record in reader:
                    dict_processing_funct(dict, record)
                    c += 1
            except Exception as e:
                print('Exception: ' + e)
                print(file_name + ': Failed on record ' + str(c))
                print(str(record))
        log(str("{: >4d}".format(len(dict))) + ' records read from "' + file_name + '"')
        return fieldnames, dict 

    def find_latest_payment(self, gsheets_rec, date_str):
        gsheets_rec['Last Payment Date'] = date_str
        last_paid_date = datetime.datetime.strptime(date_str, '%Y/%m/%d')
        tdiff = datetime.datetime.now() - last_paid_date
        if (tdiff.days > 31):
            gsheets_rec[self.get_delinquent_column_header()] = str(int(tdiff.days / 30)) # approximate months, not exact.
        else:
            gsheets_rec[self.get_delinquent_column_header()] = ''

    def merge_payment_dates(self, stripe_dict_records, paypal_dict_records):
        for r in self.gsheets_dict_records.keys():
            gsheets_rec = self.gsheets_dict_records.get(r)
            if gsheets_rec['Status'] == 'Current' and gsheets_rec.get('Expected Payment Amount') and int(gsheets_rec['Expected Payment Amount']) > 0:
                if stripe_dict_records.get(r):
                    stripe_rec = stripe_dict_records.get(r)
                    gsheets_rec['Payment Method'] = 'Stripe'
                    date_str = stripe_rec.get('Created (UTC)')
                    self.find_latest_payment(gsheets_rec, date_str)
                    gsheets_rec['Last Payment Amount'] = stripe_rec.get('Amount')
                else:
                    if paypal_dict_records.get(r):
                        paypal_rec = paypal_dict_records.get(r)
                        gsheets_rec['Payment Method'] = 'PayPal'
                        date_str = paypal_rec.get('Date')
                        self.find_latest_payment(gsheets_rec, date_str)
                        gsheets_rec['Last Payment Amount'] = paypal_rec.get('Gross') # TODO, handle new CSV format
                    else:
                        if not gsheets_rec.get('Payment Method'):
                            gsheets_rec['Payment Method'] = 'unknown'

    def write_payment_statuses(self):
        out_file_name = 'payment_statuses.csv'
        with open(out_file_name, 'w', newline='') as outfile:
            writer = csv.DictWriter(outfile, self.gsheets_fieldnames, delimiter=',', quotechar='"',
                                    quoting=csv.QUOTE_MINIMAL)
            writer.writeheader()
            for key in sorted(self.gsheets_dict_records.keys()):
                writer.writerow(self.gsheets_dict_records[key])
        log(str("{: >4d}".format(len(self.gsheets_dict_records))) + ' records written to "' + out_file_name + '"')

    def write_dict_to_csv(self, out_file_name, field_names, dict):
        with open(out_file_name, 'w', newline='') as outfile:
            writer = csv.DictWriter(outfile, field_names, delimiter=',', quotechar='"',
                                    quoting=csv.QUOTE_MINIMAL)
            writer.writeheader()
            for record in dict.items():
                writer.writerow(record[1])
        log(str("{: >4d}".format(len(dict))) + ' records written to ' + out_file_name)

    def create_new_row(self, email):
        new_row = {}
        for field in self.gsheets_fieldnames:
            new_row[field] = ''
        new_row['Email'] = email
        return new_row

    def add_unknown_stripe_emails(self, stripe_dict_records):
        for k in stripe_dict_records.keys():
            if not self.gsheets_dict_records.get(k):
                date_str = stripe_dict_records[k]['Created (UTC)']
                last_paid_date = datetime.datetime.strptime(date_str, '%Y/%m/%d')
                tdiff = datetime.datetime.now() - last_paid_date
                if (tdiff.days < 3 * 31): # consider payments within (approximately) the last three months to be "Current"
                    print('Adding member from stripe: ' + k)
                    new_row = self.create_new_row(k)
                    new_row['Last Payment Date'] = date_str
                    new_row['Last Payment Amount'] = stripe_dict_records[k]['Amount']
                    new_row['Status'] = 'Current'
                    new_row['Payment Method'] = 'Stripe'
                    self.gsheets_dict_records[k] = new_row

    def setup_columns(self):
        self.gsheets_fieldnames = [
            'First Name',
            'Family (Last) Name',
            'Email',
            'Status',
            'Expected Payment Amount',
            'Last Payment Amount',
            'Last Payment Date',
            self.get_delinquent_column_header(),
            'Payment Method',
            'Notes',
            'Membership Agreement Date',
            'Address',
            'Phone',
        ]
        i = 0
        for field_name in self.gsheets_fieldnames:
            self.field_indices[field_name] = i
            i += 1

    def print_counts(self):
        counts = {}
        for r in self.gsheets_dict_records.values():
            status = r.get('Status')
            if not counts.get(status):
                counts[status] = 1
            else:
                counts[status] += 1
        for k in counts.keys():
            print(k + ' members: ' + str(counts[k]))

    def main(self):
        self.gsheets_fieldnames, self.gsheets_dict_records = self.read_from_stream_into_dict(
                'payment statuses - payment_statuses.csv',
                self.handle_new_sheet)
        self.setup_columns()
        stripe_fieldnames, stripe_dict_records = self.read_from_stream_into_dict(
                'STRIPE_payments.csv',
                self.handle_stripe)
        paypal_fieldnames, paypal_dict_records = self.read_from_stream_into_dict(
                'PayPal_Payments.csv',
                self.handle_paypal)
        self.add_unknown_stripe_emails(stripe_dict_records)
        self.merge_payment_dates(stripe_dict_records, paypal_dict_records)
        self.write_payment_statuses()
        self.print_counts()

if '__main__' == __name__:
    Reporter().main()
