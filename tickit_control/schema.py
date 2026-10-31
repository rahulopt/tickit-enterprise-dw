"""Canonical stage physical types; checked against SQL DDL by local tests."""
SCHEMA = {
    'category': [('catid','int16'),('catgroup','string'),('catname','string'),('catdesc','string')],
    'venue': [('venueid','int16'),('venuename','string'),('venuecity','string'),('venuestate','string'),('venueseats','int32')],
    'users': [(name, kind) for name, kind in [('userid','int32'),('username','string'),('firstname','string'),('lastname','string'),('city','string'),('state','string'),('email','string'),('phone','string')]] + [(name,'bool') for name in ['likesports','liketheatre','likeconcerts','likejazz','likeclassical','likeopera','likerock','likevegas','likebroadway','likemusicals']],
    'event': [('eventid','int32'),('venueid','int16'),('catid','int16'),('dateid','int32'),('eventname','string'),('starttime','timestamp'),('venuename','string'),('venuecity','string'),('venuestate','string'),('venueseats','int32'),('catgroup','string'),('catname','string'),('catdesc','string')],
    'listing': [('listid','int32'),('sellerid','int32'),('eventid','int32'),('dateid','int32'),('numtickets','int16'),('priceperticket','decimal'),('totalprice','decimal'),('listtime','timestamp')],
    'sales': [('salesid','int32'),('listid','int32'),('sellerid','int32'),('buyerid','int32'),('eventid','int32'),('dateid','int32'),('qtysold','int16'),('pricepaid','decimal'),('commission','decimal'),('saletime','timestamp')],
}


def arrow_schema(table):
    import pyarrow as pa
    types = {'int16': pa.int16(), 'int32': pa.int32(), 'string': pa.string(),
             'bool': pa.bool_(), 'timestamp': pa.timestamp('us'), 'decimal': pa.decimal128(8,2)}
    return pa.schema([(name, types[kind]) for name, kind in SCHEMA[table]])
