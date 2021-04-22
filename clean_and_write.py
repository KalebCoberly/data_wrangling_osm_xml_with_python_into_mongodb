import re
from functools import lru_cache
import codecs
import json
import xml.etree.ElementTree as ET


# Using global constants rather than passing values around.
PHONE_RE = re.compile(r'\+1-\d\d\d-\d\d\d-\d\d\d\d')
WRONG_AC_RE = re.compile(r'1*306')

# LOWER = re.compile(r'^([a-z]|_)*$')
# LOWER_COLON = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
PROBLEMCHARS = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')
SUBNUM_RE = re.compile(r'_\d')

# These are all lists, though the extract doesn't require it for all of them.
# Another extract, to which this extract may be joined, may include features
# that span multiple lateral regions (e.g. an interstate).
# This may be a mistake, but it's just as easy to make a list with a single 
# value scalar as it is to make a scalar value a list.
IS_IN_MAP = {"is_in": ["USA", "WA", "Whatcom County", "Bellingham"],
             "is_in:country": ["USA"],
             "is_in:country_code": ["US"],
             "is_in:county": ["Whatcom"],
             "is_in:iso_3166_2": ["US:WA"],
             "is_in:state": ["WA"],
             "is_in:state_code": ["WA"]}

CREATED_LST = ["version", "changeset", "timestamp", "user", "uid"]

# Exceptions mapped to acceptable street type formats.
# Also other address abbreviations.
STREET_TYPE_MAP = {"Ave": "Avenue", "Ave.": "Avenue", "Blvd": "Boulevard",
    "Bakerview": "Bakerview Road", "Count": "Court", "Dr": "Drive",
    "Forest": "Forest Street", "Hwy": "Highway", "Meridian": "Meridian Road",
    "Pkwy": "Parkway", "Rd": "Road", "Rd.": "Road", "Road3": "Road",
    "St": "Street", "St.": "Street", "Street,": "Street", "Street\\": "Street",
    "street": "Street"}

# Keys that should be changed to another key.
WRONG_KEY_MAP = {"image": "wikimedia_commons",
                 "maxspeed:type": "source:maxspeed", "reg_name": "name",
                 "social_centre:for": "social_facility:for",
                 "symbol": "wiki:symbol"}

# Keys to store as booleans.
BOOL_TAGS_LST = ["fuel", "payment"]

# Keys to store as integers.
TO_INT_LST = ["ele", "population", "quantity", "faces", "seats", "screen",
              "lanes", "max_level", "min_level", "cables", #"voltage",
              "beds", "changing_table:count", "hoops", "disabled_spaces", "par",
              "step_count", "handicap"]

# Keys to store as floats.
TO_FLOAT_LST = ["roof:levels", "level", "building:levels:underground",
                "levels", "roof:height", "maxheight", "building:levels"]

SUBDIVIDE_LST = ["addr", "cost", "fire_hydrant", "fuel", "payment",
                 "service", "wiki"]

BAD_CHARS_LST = ["\"", "\'"]


def clean_street_type(street):
    unit = None
    street_type = street.split()[-1]
    if "#" in street_type:
        unit = street_type
        street = " ".join(street.split()[:-1])
        street_type = street.split()[-1]
    if street_type in STREET_TYPE_MAP.keys():
         street = " ".join(street.split()[:-1]) + \
            " " + STREET_TYPE_MAP[street_type]
            
    return street, unit


def audit_addr(k, v):
    unit = None # If unit number was stuck to street
    if k == "street":
        v, unit = clean_street_type(v)
    elif k == "unit" and v[:3] in STREET_TYPE_MAP.keys():
        v = STREET_TYPE_MAP[v[:3]] + v[3:]
    elif k == "housename":
        v = " ".join([word.capitalize() for word in v.split() \
                      if word != "LLC"])
    elif k == "postcode":
        v = v[:5]
        if v == "99248":
            v = "98248"
    
    return v, unit


def format_phone(num):
    formatted_num = ""
    
    if not PHONE_RE.fullmatch(num):
        formatted_num = re.sub(r'\D', "", num)
        if WRONG_AC_RE.match(formatted_num):
            formatted_num = re.sub("306", "360", formatted_num, count=1)
        if formatted_num[0] != "1":
            formatted_num = "1" + formatted_num
        if len(formatted_num) > 12:
            formatted_num = formatted_num[:11] + " x" + formatted_num[11:]
        formatted_num = "+" + formatted_num[0] + "-" + formatted_num[1:4] + "-" + formatted_num[4:7] \
            + "-" + formatted_num[7:]
        
    return formatted_num


def get_isin_set(k):
    '''
    Assumes values in this field use either commas or semicolons for
    separators.
    '''
    if "," in k:
        k = k.split(",")
    else:
        k = k.split(";")
    k = set([it.strip() for it in k])
    
    return k


def subdiv_key(k, v, subdoc_dict):
    k_split = k.split(":")
    # Base case.
    if len(k_split) == 1:
        subdoc_dict.update({k_split[0]: v})
    # Recursive case.
    else:
        # Last level?
        if k_split[0] not in subdoc_dict.keys():
            subdoc_dict.update({k_split[0]: dict()})
        # Make new key:value pair and keep drilling.
        new_k = ":".join(k_split[1:])
        new_subd_dict = subdoc_dict[k_split[0]]
        subdoc_dict[k_split[0]].update(subdiv_key(new_k, v, new_subd_dict))
    
    return subdoc_dict


def get_lstkeydict():
    return {"alt_name": list(), "animal_boarding": list(),
            "amenity": list(), "artist_name": list(),
            "bicycle:conditional": list(), "building": list(),
            "clothes": list(), "cuisine": list(), "destination": list(),
            "destination:ref": list(), "destination:ref:backward": list(),
            "destination:symbol:backward": list(),
            "destination:symbol:forward": list(), "direction": list(),
            "fax": list(), "int_name": list(), "name": list(),
            "loc_name": list(), "material": list(), "maxweight": list(),
            "maxweight:conditional": list(),
            "motor_vehicle:conditional": list(), "name_alt": list(),
            "old_name": list(), "old_railway_operator": list(),
            "old_ref": list(), "opening": list(), "opening_hours": list(),
            "phone": list(), "postal_code": list(), "seasonal": list(),
            "shop": list(), "short_name": list(), "source": list(),
            "sport": list(), "turn": list(), "turn:backward": list(),
            "turn:forward": list(), "turn:lanes": list(),
            "turn:lanes:backward": list(), "turn:lanes:forward": list(),
            "website": list()}


def handle_list_keys(v):
    '''
    Assumes values in this field only use semicolons for separators.
    '''
    lst = list()
    if v.count(";") > 0:
        lst = v.split(";")
        lst = [it.strip() for it in lst]
    else:
        lst.append(v)
        
    return lst


@lru_cache(maxsize=4)
def handle_bools(v):
    '''
    Leaves non-boolean values as is.
    '''
    v = v.lower()
    if v == 'yes' or v == 1:
        v = True
    elif v == 'no' or v == 0:
        v = False
    return v


def misc_val_edits(k, v):
    if k == "shop" and v in ["Cannabis",
                             "Parcel_Shipping"]:
        v = v.lower()
    elif k == "inscription" and v == \
    "Inscriptions too long to input, see Description.":
        v = "Inscription's too long to input; see description."
    elif k == "designation":
        v = "_".join(v.lower().split())
    elif k == "denomination" and v == "Non-denominational":
        v = "nondenominational"
    elif k == "access" and v == "privatem":
        v = "private"
    elif k == "kerb" and v == "rised":
        v = "rasied"
    elif k == "width" and v == "10'":
        v = "10 feet"
    elif k == "type":
        v == v.lower()
    elif k == "office" and v == "Whatcom_Educational_Credit_Union":
        v = "credit_union"
    elif k[:6] == "is_in" and k in IS_IN_MAP.keys():
        v = IS_IN_MAP[k]
    elif k in TO_INT_LST:
        v = int(float(v))
    elif k in TO_FLOAT_LST:
        if k == "building:levels" and v == "3s":
            v = 3
        if k == "maxheight" and \
        not any(bad_char in v for bad_char in BAD_CHARS_LST):
            v = float(v)
    
    return v
             
    
def shape_element(element):
    doc_dict = dict()
# Ignore outer elements.
    if element.tag in ["node", "way", "relation"]:
# Get attributes.
        doc_dict.update({"doc_type": element.tag})
        # Vector for lat/lon.
        pos_lst = [None,None]
        # Subdoc for creation info.
        created_dict = dict()
        for att_k, att_v in element.attrib.items():
            if att_k == "id":
                doc_dict["_id"] = att_v
            elif att_k in CREATED_LST:
                created_dict.update({att_k: att_v})
            elif att_k == "lat":
                pos_lst[0] = float(att_v)
            elif att_k == "lon":
                pos_lst[1] = float(att_v)
            else:
                doc_dict[att_k] = att_v
        if pos_lst[0] is not None and pos_lst[1] is not None:
            doc_dict["pos"] = pos_lst
        if created_dict:
            doc_dict["created"] = created_dict

# Get subelements.
        # nd elements found in way elements.
        node_refs = set()
        # member elements in relation elements.
        members = list()
        # Keys that should have lists of values.
        list_keys_dict = get_lstkeydict()
        # is_in is a special case.
        is_in = set()
        # Subdocs for subdivided keys.
        subdoc_dict = dict()
        
        # Handle subelements.
        for sub_el in element.iter():
            # Handle nd elements.
            if sub_el.tag == "nd":
                node_refs.add(sub_el.attrib["ref"])
            # Handle member elements.
            elif sub_el.tag == "member":
                members.append({"type": sub_el.attrib["type"],
                                "ref": sub_el.attrib["ref"],
                                "role": sub_el.attrib["role"]})
            # Handle tag elements.
            elif sub_el.tag == "tag":
                k = sub_el.attrib["k"]
                v = sub_el.attrib["v"]
                # Don't write tags with keys with problem characters.
                if not PROBLEMCHARS.search(k):
                    k_split = k.split(":")
                    if k == "gnis:ST_alph":
                        k = "gnis:ST_alpha"
                    elif k == "gnis:County_num" and v == "73":
                        v = "073"
                    # Other than that, don't edit tiger, gnis, nist tags.
                    elif k_split[0] not in ["tiger", "gnis", "nist"]:
                # Fix keys.
                        if k_split[0] == "contact":
                            k = ":".join(k_split[1:])
                        if SUBNUM_RE.search(k[-2:]):
                            k = k[:-2]
                            
                        # Must happen before making subdocs ("wiki").
                        if k in WRONG_KEY_MAP.keys():
                            k = WRONG_KEY_MAP[k]
                        if k in list_keys_dict.keys():
                            v = handle_list_keys(v)
                            # Format phone and fax within list creation.
                            if k in ["phone", "fax"]:
                                v = [format_phone(ph) for ph in v]
                            list_keys_dict[k].extend(v)
                        if k_split[0] in BOOL_TAGS_LST:
                                v = handle_bools(v)
                            
                # Handle subdivided keys.   
                        # Must happen after mapping wrong keys ("wiki").
                        if len(k_split) > 1 \
                        and k_split[0] in SUBDIVIDE_LST:
                        # Log if overwriting scalar.
                            if k_split[0] not in subdoc_dict.keys() \
                            and k_split[0] in doc_dict.keys():
                                print("Scalar over_written by subdoc:",
                                      k_split)
                            # addr is a special case.
                            if k_split[0] == "addr":
                                # Lose addr keys with more than one subkey.
                                # Handle street cleanup.
                                if len(k_split) == 2:
                                    v, unit = audit_addr(k_split[1], v)
                                    if unit:
                                        subdoc_dict["addr"].\
                                        update({"unit": unit})
                                    subdoc_dict = subdiv_key(k, v,
                                                             subdoc_dict)
                            else:
#                                 k = k + "_sub"
                                subdoc_dict = (subdiv_key(k, v,
                                                              subdoc_dict))
                        # Log if overwriting subdoc.
                        elif k_split[0] in subdoc_dict.keys():
                            print("Subdoc over-written by scalar:",
                                  k_split)
                        else:    
                            v = misc_val_edits(k, v)
                            doc_dict[k] = v
                    else:
                        doc_dict[k] = v
                else:
                    print("Problem characters in:", sub_el)
                    
        # Add list keys and subdocs/
        if node_refs:
            doc_dict["node_refs"] = sorted(list(node_refs))
        if members:
            doc_dict["members"] = members
        if is_in:
            doc_dict["is_in"] = sorted(list(is_in))
        for key, lst in list_keys_dict.items():
            if len(lst) > 0:
                doc_dict[key] = lst
        for subdoc_k in subdoc_dict.keys():
            doc_dict[subdoc_k] = subdoc_dict[subdoc_k]
            
        # Validate.
#         All node documents should include a position, and not include node
#         references nor members. All way documents should include node
#         references but neither a position nor members. All relation documents
#         should include members, no position, and no node references.
        if doc_dict["doc_type"] == "node":
            if any(subdoc in ["node_refs", "members"] \
                   for subdoc in doc_dict.keys()) \
            or "pos" not in doc_dict.keys():
                print("Invalid document:", doc_dict)
        elif doc_dict["doc_type"] == "way":
            if any(subdoc in ["pos", "members"] \
                   for subdoc in doc_dict.keys()) \
            or "node_refs" not in doc_dict.keys():
                print("Invalid document:", doc_dict)
        elif doc_dict["doc_type"] == "relation":
            if any(subdoc in ["pos", "node_refs"] \
                   for subdoc in doc_dict.keys()) \
            or "members" not in doc_dict.keys():
                print("Invalid document:", doc_dict)
        else:
            print("Document without type:", doc_dict)
                
    return doc_dict


        ### etc. ###
# -artist_name: make list, semicolon separated, but also comma, except in case of dates added, in which case remove and assign to art_installation_date? Use in combination with artist:wikidata=*, artist:wikipedia=*, historic=memorial, tourism=artwork
# -clothes: semicolon-separated list; 'Modern_African_Fashion' to 'modern_african_fashion' or 'modern_African_fashion'?
# -capacity: '...m^3' should be used in conjunction with man_made=storage_tank. Otherwise fields are integers, so should they be stored as such, or keep field data type consistent?
# -destination:ref capitalize directions
# -distance: default km, so strip unit and subdivide to store value as float
# -email: they all look valid, but grab a validator script anyway.
# -height: subdivide for units so values can be floats, meters by default; split by space, unit after; 'Nancy Holt, 1977'?
# -maxheight: float, meters default; should be greater than 0?
# -maxspeed; maxspeed:advisory: default kph; subdivide per unit so values can be integers? split by space. ('127' '25'? Should all be mph, divisible by 5)
# -maxstay: spell out units (min to minutes, day to days)
# -sport: make semicolon-separated list; 'beachvolleyball' to 'beach_volleyball'
# -start_date: different formats ('YYYY', 'YYYY-MM-DD', 'Month DD, YYYY'); 'Dates use the ISO 8601 system, which is based on the Gregorian calendar ... ISO8601 uses the format yyyy-mm-dd.' So, just change that one. Technically, leave '0000', but let's take a look; maybe there's a more appropriate tag.
# -turn: ('36th Street'?)
# -# turn:lanes:backward (etc.): regex (no spaces, no numbers, only given values and symbols) ('36th Street'?)
# -width: subdivide for units so values can be floats, meters by default; split by space, unit after
# -wikipedia: 'en:bellingham public library' capitalize; mixing of underscores and spaces. Replace underscores with spaces? do it in all field values? makes most sense here, since address-like values including commas.

## IRL investigations:
# -Check out fixme/FIXME
# -height: 'Nancy Holt, 1977'?
# -maxspeed; masxpeed:advisory '127' '25'?
# -maxheight: nearly all of them
# -name:en '2986'
# -opening_hours:rubbish: not properly formatted, not informative; can look these up and find out
# -start_date: '0000'
# -width: 'Cedar Jumps Green Line' (caution about width?)


def write_el(el, file_out, mode = "a", pretty = True):
    if pretty:
        with codecs.open(file_out, mode) as fo:
            fo.write(json.dumps(el, indent=2) + "\n")
    else:
        with codecs.open(file_out, mode) as fo:
            fo.write(json.dumps(el) + "\n")
    return


def process_map(file_in, fo_pre, pretty = True):
    with codecs.open(fo_pre+".json", "a") as fo:
        for _, element in ET.iterparse(file_in):
                el = shape_element(element)
                if el:
                    write_el(el=el, file_out=fo_pre+".json")

    return