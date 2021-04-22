import xml.etree.ElementTree as ET
import pandas as pd

## Elements and document structure:
# How many of each kind of element do we have?
# Which elements contain which?
# And, what are their attributes?
### Note: the osm element (i.e. the entire document) takes too much memory to
### iterate through all of its subelements. It stops iteratinf after 105
### subelements, so the list of its subelements is missing "way" and "relation".

## Tags and their keys and values:
# What types of tags do we have?
# (k attribute; "name", "service", "lanes", etc.)
# What's their cardinality? (Count of unique v attributes per k attribute.)
### Note: In cases in which the dataset is too large to read the value sets into
### memory, the code could be rewritten to just obtain the element and tag lists
### and counts. The value sets could then be pulled as needed.

def get_eldf_tagdf(filename):
    '''Given an OSM doc, gets dataframes of element counts and their subelements and attributes,
    and of tag element keys and values and counts.
    
    Parameters:
        filename: (str) filepath to .
        
    Returns:
        el_df: (pandas.DataFrame) elements.
        tag_df: (pandas.DataFrame) tags.
    '''
    # Count of each element type.
    el_count_ser = pd.Series(dtype="int", name="count")
    # Set of unique subelements types for each element type.
    el_subel_ser = pd.Series(dtype="object", name="sub_els")
    # Set of unique attributes for each element type.
    el_attr_ser = pd.Series(dtype="object", name="attributes")

    # Count of times tag key is used. (count per "k")
    count_ser = pd.Series(dtype="int", name="tag_use_count")
    # Set of unique tag values. (unique "v" per "k")
    set_ser = pd.Series(dtype="object", name="val_set")
    # Count of unique tag values.
    uniq_ser = pd.Series(dtype="int", name="uniq_count")
    # Ratio of tag key's times used to tag's unique values.
    # (count 'k' per count unique 'v')
    ratio_ser = pd.Series(dtype="float", name="usage_per_uniq")

#     osm_sub_count = 0
    for _, el in ET.iterparse(source=filename, events=("start",)):
    # Catalog/count the elements, and catalog their unique subelements
        # and attributes.
        if el.tag not in el_count_ser.index:
            el_count_ser[el.tag] = 0
            el_subel_ser[el.tag] = set()
            el_attr_ser[el.tag] = set()
        el_count_ser[el.tag] += 1
        for att_k in el.attrib.keys():
            el_attr_ser[el.tag].add(att_k)
        for sub_el in el.iter():
#             if el.tag == "osm":
#                 osm_sub_count += 1
            if el.tag != sub_el.tag:
                el_subel_ser[el.tag].add(sub_el.tag)
    # Catalog/count tag keys, and catalog/count their unique values.
        if el.tag == "tag":
            if el.attrib["k"] not in count_ser.index:
                count_ser[el.attrib["k"]] = 0
                set_ser[el.attrib["k"]] = set()
            count_ser[el.attrib["k"]] += 1
            set_ser[el.attrib["k"]].add(el.attrib["v"])

    # Element dataframe.           
    el_df = pd.concat([el_count_ser, el_subel_ser, el_attr_ser], axis=1)
    el_df.index.name = "element_type"

    # Tag dataframe.
    tag_df = pd.concat([count_ser, set_ser, uniq_ser, ratio_ser], axis=1)
    tag_df.index.name = "tag_key" # ("k")
    # Get count of unique values. ("v")
    tag_df["uniq_count"] = tag_df["val_set"].apply(len)
    # Get ratio of unique values to tag uses.
    tag_df["usage_per_uniq"] = tag_df["tag_use_count"] / tag_df["uniq_count"]
    
    return el_df, tag_df#, osm_sub_count