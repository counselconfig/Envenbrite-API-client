import requests
from requests.adapters import HTTPAdapter
from requests import exceptions
import json
import pandas as pd
import re
from datetime import datetime
from dateutil.relativedelta import relativedelta
import dateutil.parser
import time
import argparse


class SSLContextAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        from requests.packages.urllib3.util.ssl_ import create_urllib3_context
        context = create_urllib3_context()
        kwargs['ssl_context'] = context
        context.load_default_certs() # this loads the OS defaults on Windows
        return super(SSLContextAdapter, self).init_poolmanager(*args, **kwargs)


class SSLContextAdapterLocal(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        import ssl

        # context = create_urllib3_context()
        context = ssl.create_default_context() # Remove
        kwargs['ssl_context'] = context
        # context.load_default_certs() # this loads the OS defaults on Windows
        return super(SSLContextAdapterLocal, self).init_poolmanager(*args, **kwargs)


api_key = 'add key here'
headers = {'Content-Type': 'application/json',
           'Authorization': f'Bearer {api_key}'}
base_url = 'https://www.eventbriteapi.com'
base_api_url = f'{base_url}/v3'
#https_proxy = 'https://...:8080'
# This proxy is meant to be more permissive but doesn't seem to work
#https_proxy = 'https://...:8080'
organiser = [add 11 number int]

columns = [
    'Event Name',
    'Event ID',
    'Order no.',
    'Order Date',
    'Prefix',
    'First Name',
    'Surname',
    'Email',
    'Quantity',
    'Ticket Type',
    'Venue Name',
    'Venue No.',
    'Organiser Name',
    'Attendee no.',
    'Barcode no.',
    'Buyer Surname',
    'Buyer First Name',
    'Buyer Email',
    'Date Attending',
    'Device Name',
    'Check-In Date',
    'Discount',
    'Hold',
    'Order Type',
    'Total Paid',
    'Eventbrite Fees',
    'Eventbrite Payment Processing',
    'Attendee Status',
    'Delivery Method',
    'Home Address 1',
    'Home Address 2',
    'Home City',
    'County of Residence',
    'Home Postcode',
    'Home Country',
    'Gender',
    'Age',
    'Birth Date',
    'Would you like to receive email updates from The National Archives?',
    'Would you like to receive our free enewsletter and emails about news, products and services from The National Archives?',
    'Join our mailing list',
    'How did you hear about this event?',
    'Billing Address 1',
    'Billing Address 2',
    'Billing City',
    'Billing State/Province/County',
    'Billing Postcode',
    'Billing Country',
    'Organiser No.',
    'Capacity',
    'What is your main reason for booking a ticket to this event?',
]

renamed_columns = [
    'Event.Name',
    'Event.ID',
    'Order.no.',
    'Order.Date',
    'Prefix',
    'First.Name',
    'Surname',
    'Email',
    'Quantity',
    'Ticket.Type',
    'Venue.Name',
    'Venue.No.',
    'Organiser.Name',
    'Attendee.no.',
    'Barcode.no.',
    'Buyer.Surname',
    'Buyer.First.Name',
    'Buyer.Email',
    'Date.Attending',
    'Device.Name',
    'Check.In.Date',
    'Discount',
    'Hold',
    'Order.Type',
    'Total.Paid',
    'Eventbrite.Fees',
    'Eventbrite.Payment.Processing',
    'Attendee.Status',
    'Delivery.Method',
    'Home.Address.1',
    'Home.Address.2',
    'Home.City',
    'County.of.Residence',
    'Home.Postcode',
    'Home.Country',
    'Gender',
    'Age',
    'Birth.Date',
    'Would.you.like.to.receive.email.updates.from.The.National.Archives.',
    'Would.you.like.to.receive.our.free.enewsletter.and.emails.about.news..products.and.services.from.The.National.Archives.',
    'Join.our.mailing.list',
    'How.did.you.hear.about.this.event.',
    'Billing.Address.1',
    'Billing.Address.2',
    'Billing.City',
    'Billing.State.Province.County',
    'Billing.Postcode',
    'Billing.Country',
    'Organiser.No.',
    'Capacity',
    'What.is.your.main.reason.for.booking.a.ticket.to.this.event.',
]


def remove_none(d):
    return {key: value for key, value in d.items() if value}


def parse_events(events):
    return [parse_event(event) for event in events]


def parse_event(event):
    """
    Unpacks the relevant event information to a 'flat' object
    :param event:
    :return:
    """
    # Remove None values. This ensures that we can use .get to fallback
    # to defaults.
    event = remove_none(event)

    # Convert date strings to datetime
    date_attending = pd.to_datetime(
        event.get('start', {}).get('local', ''),
        format='%Y-%m-%dT%H:%M:%S'
    )

    utc = pd.to_datetime(
        event.get('start', {}).get('utc', ''),
        format='%Y-%m-%dT%H:%M:%SZ'
    )

    # Calculate time offset
    time_offset = diff_in_hours(date_attending - utc)

    # Convert to strings
    date_attending_s = dateattending_to_string(date_attending)

    # Fix event name if it is not None, i.e. remove newlines and
    # trailing spaces.
    event_name = event.get('name', {}).get('text', '')
    if event_name is not None:
        event_name = event_name.replace('\n', '').strip()

    return {
        'Event ID': event.get('id', ''),
        # Remove new lines from event titles
        'Event Name': event_name,
        'Venue Name': event.get('venue', {}).get('name', ''),
        'Venue No.': event.get('venue_id', ''),
        'Organiser Name': event.get('organizer', {''}).get('name', ''),
        'Organiser No.': event.get('organizer_id', ''),
        'Capacity': event.get('capacity', ''),
        'timezone': event.get('start', {}).get('timezone', ''),
        'Date Attending': date_attending_s,
        'Date Attending Date': date_attending,
        'time_offset': time_offset,
        'status': event.get('status', '')
    }


def dateattending_to_string(dt_s):
    dt = pd.to_datetime(
        dt_s,
        format='%Y-%m-%dT%H:%M:%SZ'
    )
    # No need to add offset, Date Attending in local time
    # Format string
    return dt.strftime(format="%b %d, %Y at %I:%M %p")


def parse_orders(orders, time_offset):
    return [attendee for order in orders
            for attendee in parse_order(order, time_offset)
            if relevant_attendee(attendee)]


def parse_order(order, time_offset):
    order = remove_none(order)
    attendees = [remove_none(attendee) for attendee in order['attendees']]
    return [parse_attendee(order, attendee, time_offset) for attendee in attendees]


def relevant_attendee(attendee):
    if attendee['Attendee Status'] == 'Deleted':
        return False
    if attendee['Guest list ID'] != '':
        return False
    return True


def parse_attendee(order, attendee, time_offset):
    # Handle barcode
    barcode_status = ''
    barcode = ''
    check_in_date_s = ''
    if 'barcodes' in attendee and len(attendee['barcodes']):
        barcode_status = attendee['barcodes'][0].get('status')
        barcode = attendee['barcodes'][0].get('barcode', '')
        check_in_date_s = attendee['barcodes'][0].get('changed', '')

    # Handle dates
    order_date = ''
    if 'created' in order:
        order_date = orderdate_to_string(order['created'], time_offset)
    check_in_date = ''
    if barcode_status == 'used' and check_in_date_s != '':
        check_in_date = checkindate_to_string(check_in_date_s, time_offset)

    # These are all the questions that we are interested in
    relevant_questions = {
        'How did you hear about this event?',
        'How did you hear about this event? ',
        'What is your main reason for booking a ticket to this event?',
        'Would you like to receive email updates from The National Archives?',
        'Would you like to receive our free enewsletter and emails about news, products and services from The National Archives?',
        "Would you like to receive The National Archives' free monthly enewsletter, featuring regular news, updates, events and offers?",
        'Join our mailing list',
        'Subscribe to The National Archives’ mailing list to receive regular news, updates and priority booking for events',
        'Why are you booking a ticket to this event? Please tick all that apply'
    }


    # Get the relevant questions
    questions = {
        answer['question']: answer.get('answer', '')
        for answer in attendee.get('answers', [])
        if answer['question'] in relevant_questions and 'answer' in answer
    }

    ## Remappings:
    # question1 should be mapped to question2
    question_from1 = "Would you like to receive The National Archives' free monthly enewsletter, featuring regular news, updates, events and offers?"
    question_from2 = "Subscribe to The National Archives’ mailing list to receive regular news, updates and priority booking for events"
    question2 = 'Would you like to receive email updates from The National Archives?'
    if question_from1 in questions and question2 not in questions:
        questions[question2] = questions[question_from1]
    if question_from2 in questions and question2 not in questions:
        questions[question2] = questions[question_from2]
    
    # 
    question_from_3 = "Why are you booking a ticket to this event? Please tick all that apply"
    question_to_3 = "What is your main reason for booking a ticket to this event?"
    if question_from_3 in questions and question_to_3 not in questions:
        questions[question_to_3] = questions[question_from_3]

    question_from_4 = "How did you hear about this event? "
    question_to_4 = "How did you hear about this event?"
    if question_from_4 in questions and question_to_4 not in questions:
        questions[question_to_4] = questions[question_from_4]

    # These are the output questions, we need to make sure they're in the
    # data set
    output_questions = {
        'How did you hear about this event?',
        'What is your main reason for booking a ticket to this event?',
        'Would you like to receive email updates from The National Archives?',
        'Would you like to receive our free enewsletter and emails about news, products and services from The National Archives?',
        'Join our mailing list',
    }
    for output_question in output_questions:
        if output_question not in questions:
            questions[output_question] = ''

    status = attendee.get('status', '')
    # Seems to be a bug in the API, sometimes checked in attendees don't
    # have status = "Checked In", so we fix that here
    if barcode_status == 'used':
        status = 'Checked In'
    elif barcode_status == 'unused' and status == 'Checked In':
        status = 'Attending'

    if status == 'Transferred':
        # We don't see this status in the manual export, and they all
        # correspond to Not Attending
        status = 'Not Attending'

    ret = {
        'Order no.': order.get('id', ''),
        'Event ID': order.get('event_id', ''),
        'Order Date': order_date,
        'Prefix': attendee.get('profile', {}).get('prefix', ''),
        'First Name': attendee.get('profile', {}).get('first_name', ''),
        'Surname': attendee.get('profile', {}).get('last_name', ''),
        'Email': attendee.get('profile', {}).get('email', ''),
        'Quantity': attendee.get('quantity', 1),
        'Ticket Type': attendee.get('ticket_class_name', ''),
        'Attendee no.': attendee.get('id', ''),
        'Barcode no.': barcode,
        'Buyer Surname': order.get('last_name', ''),
        'Buyer First Name': order.get('first_name', ''),
        'Buyer Email': order.get('email', ''),
        'Device Name': '',
        'Check-In Date': check_in_date,
        'Discount': '',  # Not used, left empty
        'Hold': '',  # Not sure what this column is supposed to be
        'Order Type': '',  # Not used, left empty
        'Total Paid': attendee.get('costs', {}).get('gross', {}).get('major_value', ''),
        'Eventbrite Fees': attendee.get('costs', {}).get('eventbrite_fee', {}).get('major_value'),
        'Eventbrite Payment Processing': attendee.get('costs', {}).get('payment_fee', {}).get('major_value'),
        'Attendee Status': status,
        'Delivery Method': attendee.get('delivery_method', ''),
        'Gender': attendee.get('profile', {}).get('gender', ''),
        'Age': attendee.get('profile', {}).get('age', ''),
        'Birth Date': attendee.get('profile', {}).get('birth_date', ''),
        # Guest list ID is only used to filter out 'Guest' tickets
        'Guest list ID': attendee.get('guestlist_id', ''),
        'Home Address 1': attendee.get('profile', {}).get('addresses', {}).get('home', {}).get('address_1', ''),
        'Home Address 2': attendee.get('profile', {}).get('addresses', {}).get('home', {}).get('address_2', ''),
        'Home City': attendee.get('profile', {}).get('addresses', {}).get('home', {}).get('city', ''),
        'County of Residence': attendee.get('profile', {}).get('addresses', {}).get('home', {}).get('region', ''),
        'Home Postcode': attendee.get('profile', {}).get('addresses', {}).get('home', {}).get('postal_code', ''),
        'Home Country': attendee.get('profile', {}).get('addresses', {}).get('home', {}).get('country', ''),
        'Billing Address 1': attendee.get('profile', {}).get('addresses', {}).get('bill', {}).get('address_1', ''),
        'Billing Address 2': attendee.get('profile', {}).get('addresses', {}).get('bill', {}).get('address_2', ''),
        'Billing City': attendee.get('profile', {}).get('addresses', {}).get('bill', {}).get('city', ''),
        'Billing State/Province/County': attendee.get('profile', {}).get('addresses', {}).get('bill', {}).get('region', ''),
        'Billing Postcode': attendee.get('profile', {}).get('addresses', {}).get('bill', {}).get('postal_code', ''),
        'Billing Country': attendee.get('profile', {}).get('addresses', {}).get('bill', {}).get('country', '')
    }

    # Bring it all together
    attendee_data = {**ret, **questions}

    return attendee_data


def parse_attendee_answers(raw_orders):
    questions = [
        {
            'Order no': order.get('id', ''),
            'Event ID': order.get('event_id', ''),
            'Attendee no.': attendee.get('id', ''),
            'Question': answer.get('question', ''),
            'Answer': answer.get('answer', ''),
            'Question ID': answer.get('question_id',''),
            'Type': answer.get('type', '')
        }
        for order in raw_orders
        for attendee in order['attendees']
        for answer in attendee.get('answers', [])
        if 'answer' in answer and answer['answer'] != ''
    ]

    return questions


def orderdate_to_string(dt_s, time_offset):
    # Parse datetime
    dt = pd.to_datetime(
        dt_s,
        format='%Y-%m-%dT%H:%M:%SZ'
    )
    # Add the time offset
    dt = dt + pd.Timedelta(hours=time_offset)
    # Format string
    base = dt.strftime(format="%Y-%m-%d %H:%M:%S")
    sign = '+' if time_offset >= 0 else '-'
    # Need to remove sign from offset:
    time_offset = time_offset if time_offset >= 0 else -time_offset
    return f'{base}{sign}{time_offset:02d}:00'


def checkindate_to_string(dt_s, time_offset):
    dt = pd.to_datetime(
        dt_s,
        format='%Y-%m-%dT%H:%M:%SZ'
    )
    # Add the time offset
    dt = dt + pd.Timedelta(hours=time_offset)
    # Format string
    return dt.strftime(format="%B %d, %Y %I:%M %p")


def has_events_beyond_cutoff(results):
    """
    Checks the results to see if there are any results
    older than the cut-off (3 years). If so, we return
    True to indicate that we can stop asking for events
    information. Used in get_events_for_organisation
    below.
    """
    for event in results:
        try:
            start_time = date_attending = pd.to_datetime(
                event.get('start', {}).get('local', ''),
                format='%Y-%m-%dT%H:%M:%S'
            )
        except:
            print(json.dumps(event))
            raise

        if start_time <= datetime.now() - relativedelta(years=3):
            return True
    
    return False

def get_events_for_organisation(adapter, organisation_id):
    print('Getting events.')
    stop, raw_results = continuation_call(
        adapter,
        get_events_for_organisation_url(organisation_id),
        'events',
        has_events_beyond_cutoff
    )

    results = parse_events(raw_results)
    return stop, raw_results, results


def get_orders_and_answers_for_event(adapter, event_id, time_offset):
    print(f'Getting orders for event {event_id}.')
    stop, raw_results = continuation_call(
        adapter,
        get_orders_with_attendees_url(event_id),
        'orders',
    )

    print('Parsing orders...')
    results = parse_orders(raw_results, time_offset)
    answers = parse_attendee_answers(raw_results)

    return stop, raw_results, results, answers


def continuation_call(adapter, get_url, key, should_stop=lambda result: False):
    """
    get_url: Function that takes one optional argument, the contiuation code.
             If that argument is
             missing then it returns the base URL, but if it's supplied then
             the contiuation code is appended to the base URL.
    key    : The key to the relevant object in the result, either 'events'
             or 'orders'.
    should_stop: A function which takes a result object and determines
                 whether we should keep calling or stop. Defaults to
                 continuing.
    """
    print(f'Getting first page')
    result = call(adapter, get_url())
    if result is None:
        return True, []
    stop = False
    if key not in result:
        raise Exception(f'No {key} in result: {result}.')
    values = result[key]
    if 'pagination' in result:
        print(f'Read {len(values)} {key} values (page {result["pagination"]["page_number"]} of {result["pagination"]["page_count"]}).')
    else:
        print(f'Read {len(values)} {key} values.')
    ret = values
    if 'pagination' in result:
        while result['pagination']['has_more_items']:
            result = call(adapter, get_url(result["pagination"]["continuation"]))
            if result is None:
                stop = True
                break
            if key not in result:
                raise Exception(f'No {key} in result: {result}.')
            values = result[key]
            print(f'Read {len(values)} {key} values (page {result["pagination"]["page_number"]} of {result["pagination"]["page_count"]}).')
            ret += values
            if should_stop(result[key]):
                print('Stopping flag set, stopping.')
                break

    return stop, ret


def get_events_for_organisation_url(organisation_id):
    def f(continuation=None):
        if continuation is None:
            return f'{base_api_url}/organizations/{organisation_id}/events?expand=organizer,venue&order_by=start_desc'
        else:
            return f'{base_api_url}/organizations/{organisation_id}/events?expand=organizer,venue&order_by=start_desc&continuation={continuation}'
    return f


def get_orders_with_attendees_url(event_id):
    def f(continuation=None):
        if continuation is None:
            return f'{base_api_url}/events/{event_id}/orders?expand=attendees,answers'
        else:
            return f'{base_api_url}/events/{event_id}/orders?expand=attendees,answers&continuation={continuation}'
    return f


def hit_rate_limit(result):
    """Helper function which checks if we've hit the rate limit"""
    return result.get('status_code', 0) == 429


def call(adapter, url):
    keep_trying = True
    response = None
    error = False
    while keep_trying:
        try:
            s = requests.Session()
            s.mount(base_url, adapter)
            response = s.get(url, headers=headers) #, proxies={'https': https_proxy})
            result = json.loads(response.content.decode('utf-8'))
            if hit_rate_limit(result):
                while hit_rate_limit(result):
                    print('Hit rate limit, waiting for 2 minutes...')
                    time.sleep(60 * 2)
                    response = s.get(url, headers=headers) #, proxies={'https': https_proxy})
                    result = json.loads(response.content.decode('utf-8'))
            return result
        except exceptions.SSLError:
            print('SSL error, please refresh https://www.eventbriteapi.com')
            i = input('Contiue (y/n)? ')
            if i == 'n' or i == 'N':
                print('Stopping')
                keep_trying = False
                error = True
            raise
        except json.decoder.JSONDecodeError:
            print('------------- Bad response --------------')
            print(response.content)
            print('----------- End bad response ------------')
            i = input('Contiue (y/n)? ')
            if i == 'n' or i == 'N':
                print('Stopping')
                keep_trying = False
                error = True

    return None
  

def diff_in_hours(timedelta):
    diff_in_seconds = timedelta.seconds + timedelta.days * (60 * 60 * 24)
    return diff_in_seconds // (60 * 60)


def relevant_event(row):
    if pd.isnull(row['Event Name']):
        return False
    elif row['status'] in {'draft', 'canceled'}:
        # Ignore draft or canceled events
        return False
    elif re.match('Booking a coach bay.*', row['Event Name']):
        # Ignore all Booking a coach bay events
        return False
    elif row['Organiser Name'] == 'Education Online':
        return False
    elif row['Organiser Name'] == 'The National Archives: for schools':
        return False
    elif row['Organiser Name'] == 'Internal Comms':
        return False
    elif row['Organiser Name'] == 'Test':
        return False
    elif 'cancelled' in row['Event Name'].lower():
        return False
    elif row['Date Attending Date'] <= datetime.now() - relativedelta(years=3):
        # Only include recent events (within the past 3 years)
        return False
    else:
        return True


def read_events(adapter, output_filename, answers_output_filename):
    print(f'Getting events for organisation {organiser}.')
    _, _, events_json = get_events_for_organisation(adapter, organiser)
    events_df = pd.DataFrame(events_json)
    events_df.to_csv('events.csv', index=False)
    # events_df = pd.read_csv('events.csv')
    events_df['Date Attending Date'] = pd.to_datetime(events_df['Date Attending Date'])
    events_df['Event ID'] = events_df['Event ID'].astype(str)

    print(f'Read {len(events_df)} events.')

    removed_events_df = events_df[[not relevant_event(row) for _, row in events_df.iterrows()]]
    removed_events_df.to_csv('removed_events.csv', index=False)

    # Filter out irrelevant events
    events_df = events_df[[relevant_event(row) for _, row in events_df.iterrows()]]
    raw_orders = []
    orders = []
    answers = []
    nr_events = len(events_df)

    print(f'Kept {nr_events} after filtering out events older than 3 years.')

    start = time.time()
    for (i, (index, row)) in enumerate(events_df.iloc[::-1].iterrows()):
        print('Processing...')
        if i > 0:
            elapsed = time.time() - start
            remaining = (elapsed / i) * (nr_events - i)
            print(f'Processing event {i+1} of {nr_events} ({i/nr_events*100:.2f}%, ' +
                  f'estimated time remaining {remaining//60:.0f} m {remaining % 60:.0f} s, '
                  f'elapsed time {elapsed//60:.0f} m {elapsed % 60:.0f} s).')
        else:
            print(f'Processing event {i+1} of {nr_events} '
                  f'({i/nr_events*100:.2f}%).')

        stop, new_raw_orders, new_orders, new_answers = get_orders_and_answers_for_event(adapter, row['Event ID'], row['time_offset'])
        raw_orders += new_raw_orders
        orders += new_orders
        answers += new_answers
        if stop:
            break

    print(f'Processing all events took {time.time() - start}')
    orders_df = pd.DataFrame(orders)

    # Join event data to order data
    all_df = pd.merge(events_df, orders_df, left_on='Event ID', right_on='Event ID')

    # Only keep relevant columns
    all_df = all_df[columns]
    # Rename columns
    all_df.columns = renamed_columns
    # Save as excel
    all_df.to_excel(output_filename, sheet_name='Sheet 1', index=False)

    # Save events with no tickets
    no_tickets_df = events_df[~events_df['Event ID'].isin(all_df['Event.ID'])]
    no_tickets_df.to_csv('events_no_tickets.csv', index=False)

    # Save answers
    if answers_output_filename is not None:
        pd.DataFrame(answers).to_excel(answers_output_filename, sheet_name='Sheet 1', index=False)


def main():
    args = _parse_args()

    if args.local:
        print('Using local SSL')
        adapter = SSLContextAdapterLocal()
    else:
        print('using SSLContextAdapter')
        adapter = SSLContextAdapter()

    if args.event is not None:
        # We're only looking up a single event, time offset is set to 0
        _, raw_orders, orders, answers = get_orders_and_answers_for_event(adapter, args.event, 0)
        pd.DataFrame(orders).to_excel(args.output, sheet_name='Sheet 1', index=False)
        if 'answers_output' in args and args.answers_output is not None:
            pd.DataFrame(answers).to_excel(args.answers_output, sheet_name='Sheet 1', index=False)
        if 'json_event_output' in args and args.json_event_output is not None:
            with open(args.json_event_output, 'w') as file:
                json.dump(raw_orders, file, sort_keys=True, indent=2)
        else:
            print('Not saving json')
    else:
        # We're doing a "standard run", do the full process
        read_events(adapter, args.output, args.answers_output)


def _parse_args():
    parser = argparse.ArgumentParser(
        description='Calls Eventbrite API.')
    parser.add_argument(
        '-o',
        '--output',
        help='location of output Excel file',
        type=str,
        required=True
    )
    parser.add_argument(
        '-a',
        '--answers-output',
        help='locations of answers output Excel file',
        type=str
    )
    parser.add_argument(
        '-e',
        '--event',
        help='single event ID to look up',
        type=str
    )
    parser.add_argument(
        '-l',
        '--local',
        action='store_true',
        help='If passed, uses urlib3 to create SSL context, only to be used when running locally'
    )
    parser.add_argument(
        '-j',
        '--json-event-output',
        help='Output filename for single event JSON (only used if event ID is passed)'
    )

    return parser.parse_args()


if __name__ == "__main__":
    main()
