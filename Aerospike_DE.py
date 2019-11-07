# -*- coding: utf-8 -*
from __future__ import print_function
import aerospike
from aerospike  import predicates as p
import logging
import pprint
import sys

try:
    config = {
        'hosts': [ ('127.0.0.1', 3000) ]
    }
    client = aerospike.client(config).connect()
except Exception as e:
    print("error: {0}".format(e), file=sys.stderr)
    sys.exit(1)

def add_customer(customer_id, phone_number, lifetime_value):
  key = ('test', 'demo', customer_id)
  try:
    client.put(key, {
    'phone': phone_number, 
    'ltv': lifetime_value})
  except Exception as e:
   import sys
   print("error: {0}".format(e), file=sys.stderr)

def get_ltv_by_id(customer_id):
    key = ('test', 'demo', customer_id)
    (key, metadata, record) = client.get(key)
    if(record == {}):
     logging.error('Requested non-existent customer ' + str(customer_id))
    else:
    return record.get('ltv')

    client.index_integer_create('test', 'demo', 'phone', 'phone')
def get_ltv_by_phone(phone_number):
    query = client.query('test', 'demo')   
    query.select('ltv')
    query.where(p.equals('phone', phone_number))
    ltv_phone = []
    def matched_names((key, metadata, bins)):
        ltv_phone.append(bins['ltv'])
    query.foreach(matched_names,  {'total_timeout':2000} )
    return ltv_phone
    
    client.close()