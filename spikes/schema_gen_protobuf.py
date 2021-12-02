import importlib
import json
import os
import pkgutil
import re
from google.ads.googleads.v9.resources.types import campaign, ad, ad_group, customer
from google.protobuf.pyext.cpp_message import GeneratedProtocolMessageType
from google.protobuf.pyext._message import RepeatedScalarContainer, RepeatedCompositeContainer

#>>> type(campaign.Campaign()._pb.target_spend.__class__)
#<class 'google.protobuf.pyext.cpp_message.GeneratedProtocolMessageType'>

# Unknown types lookup to their actual class in the code. For some reason these differ.
# Unknown classes should be found in this project, likely somehwere around here:
# https://github.com/googleads/google-ads-python/tree/14.1.0/google/ads/googleads/v9/common/types
type_lookup = {"google.ads.googleads.v9.common.FinalAppUrl": "google.ads.googleads.v9.common.types.final_app_url.FinalAppUrl",
               "google.ads.googleads.v9.common.AdVideoAsset": "google.ads.googleads.v9.common.types.ad_asset.AdVideoAsset",
               "google.ads.googleads.v9.common.AdTextAsset": "google.ads.googleads.v9.common.types.ad_asset.AdTextAsset",
               "google.ads.googleads.v9.common.AdMediaBundleAsset": "google.ads.googleads.v9.common.types.ad_asset.AdMediaBundleAsset",
               "google.ads.googleads.v9.common.AdImageAsset": "google.ads.googleads.v9.common.types.ad_asset.AdImageAsset",

               "google.ads.googleads.v9.common.PolicyTopicEntry": "google.ads.googleads.v9.common.types.policy.PolicyTopicEntry",
               "google.ads.googleads.v9.common.PolicyTopicConstraint": "google.ads.googleads.v9.common.types.policy.PolicyTopicConstraint",
               "google.ads.googleads.v9.common.PolicyTopicEvidence": "google.ads.googleads.v9.common.types.policy.PolicyTopicEvidence",
               "google.ads.googleads.v9.common.PolicyTopicConstraint.CountryConstraint": "google.ads.googleads.v9.common.types.policy.PolicyTopicConstraint", # This one's weird, handling it manually in the generator

               "google.ads.googleads.v9.common.UrlCollection": "google.ads.googleads.v9.common.types.url_collection.UrlCollection",

               "google.ads.googleads.v9.common.CustomParameter": "google.ads.googleads.v9.common.types.custom_parameter.CustomParameter",

               "google.ads.googleads.v9.common.ProductImage": "google.ads.googleads.v9.common.types.ad_type_infos.ProductImage",
               "google.ads.googleads.v9.common.ProductVideo": "google.ads.googleads.v9.common.types.ad_type_infos.ProductVideo",

               "google.ads.googleads.v9.common.FrequencyCapEntry": "google.ads.googleads.v9.common.types.frequency_cap.FrequencyCapEntry",

               "google.ads.googleads.v9.common.TargetRestriction": "google.ads.googleads.v9.common.types.targeting_setting.TargetRestriction",
               "google.ads.googleads.v9.common.TargetRestrictionOperation": "google.ads.googleads.v9.common.types.targeting_setting.TargetRestrictionOperation",
               }

def import_class(base_module_name):
    dir_path = os.path.dirname(os.path.realpath(base_path))
    sub_modules = [n for _f,n,_p in pkgutil.walk_packages(path=[dir_path])
               if n not in ['base', 'spec']]
    for name in sub_modules:
        dynamic_module = base_module_name + '.' + name
        try:
            mod = importlib.import_module(dynamic_module)
        except ImportError as ex:
            print("WARNING Unable to import module: {}, Error is: {}".
                  format(dynamic_module, ex))
            continue
        BaseTapTest = getattr(mod, 'BaseTapTest', None)
        unittest_exists = getattr(mod, 'unittest', None)
        TestCase_exists = getattr(mod, 'TestCase', None)
        if BaseTapTest is None and unittest_exists is None and TestCase_exists is None:
            # Indicates we're not in a module with a test that inherits from the base test or TestCase
            continue
        tests_in_module = [a for a in dir(mod) if (inspect.isclass(getattr(mod, a)) and
                                                   (safe_issubclass(getattr(mod, a), BaseTapTest) or
                                                    safe_issubclass(getattr(mod, a), TestCase)) and
                                                   a != 'BaseTapTest')]
        for test in tests_in_module:
            setattr(sys.modules[base_module_name], test, getattr(mod, test))

# From: https://stackoverflow.com/questions/19053707/converting-snake-case-to-lower-camel-case-lowercamelcase
def to_camel_case(snake_str):
    components = snake_str.split('_')
    # We capitalize the first letter of each component except the first one
    # with the 'title' method and join them together.
    return components[0] + ''.join(x.title() for x in components[1:])

def type_to_json_schema(typ):
    # TODO: Bytes in an anyOf gives us, usually, just 'string', so it can be a non-anyOf?
    if typ == 'bytes':
        return {"type": ["null","UNSUPPORTED_string"]}
    elif typ == 'int':
        return {"type": ["null","integer"]}
    elif typ in ['str','unicode']:
        return {"type": ["null","string"]}
    elif typ == 'long':
        return {"type": ["null","integer"]}
    else:
        raise Exception(f"Unknown scalar type {typ}")

def handle_scalar_container(acc, prop_val, prop_camel):
    try:
        prop_val.append(1)
        prop_val.append(True)
        prop_val.append(0.0)
    except TypeError as e:
        re_result = re.search(r"expected (.+) got ", str(e))
        if re_result:
            actual_type = re_result.groups()[0]
            mod = importlib.import_module('.'.join(actual_type.split('.')[:-1]))
            obj = getattr(mod, actual_type.split('.')[-1])()
            acc[prop_camel] = {"type": ["null", "array"],
                               "items": get_schema({},obj._pb)}
        else:
            actual_types = re.search(r"but expected one of: (.+)$", str(e)).groups()[0].split(',')
            actual_types = [t.strip() for t in actual_types]
            acc[prop_camel] = {"type": ["null", "array"],
                               "items": {"anyOf": [type_to_json_schema(t) for t in actual_types]}}


def handle_composite_container(acc, prop_val, prop_camel):
    try:
        prop_val.append(1)
        prop_val.append(True)
        prop_val.append(0.0)
    except TypeError as e:
        re_result = re.search(r"expected (.+) got ", str(e))
        if not re_result:
            import ipdb; ipdb.set_trace()
            1+1
            raise
        shown_type = re_result.groups()[0]
        actual_type = type_lookup.get(shown_type)
        if not actual_type:
            print(f"Unknown composite type: {shown_type}")
        else:
            # TODO: Should we just build the objects based on the type name and insert them as a definition and $ref to save space?
            mod = importlib.import_module('.'.join(actual_type.split('.')[:-1]))
            if shown_type == "google.ads.googleads.v9.common.PolicyTopicConstraint.CountryConstraint":
                obj = getattr(mod, actual_type.split('.')[-1]).CountryConstraint()
            else:
                obj = getattr(mod, actual_type.split('.')[-1])()
            acc[prop_camel] = {"type": ["null", "array"],
                               "items": get_schema({},obj._pb)}


def get_schema(acc, current):
    for prop in filter(lambda p: re.search(r"^[a-z]", p), dir(current)):
        try:
            prop_val = getattr(current, prop)
            prop_camel = to_camel_case(prop)
            if isinstance(prop_val.__class__, GeneratedProtocolMessageType):
                # TODO: Should we just build the objects based on the type name and insert them as a definition and $ref to save space?
                new_acc_obj = {}
                acc[prop_camel] = {"type": ["null", "object"],
                                   "properties": get_schema(new_acc_obj, prop_val)}
            elif isinstance(prop_val, bool):
                acc[prop_camel] = {"type": ["null", "boolean"]}
            elif isinstance(prop_val, str):
                acc[prop_camel] = {"type": ["null", "string"]}
            elif isinstance(prop_val, int):
                acc[prop_camel] = {"type": ["null", "integer"]}
            elif isinstance(prop_val, float):
                acc[prop_camel] = {"type": ["null", "string"],
                                   "format": "singer.decimal"}
            elif isinstance(prop_val, bytes):
                # TODO: Should this just be empty? Then put it elsewhere to mark as unsupported? With a message?
                # - Or should we just make it string?
                acc[prop_camel] = {"type": ["null", "UNSUPPORTED_string"]}
            elif isinstance(prop_val, RepeatedScalarContainer):
                handle_scalar_container(acc, prop_val, prop_camel)
            elif isinstance(prop_val, RepeatedCompositeContainer):
                handle_composite_container(acc, prop_val, prop_camel)
            else:
                import ipdb; ipdb.set_trace()
                1+1
                raise Exception(f"Unhandled type {type(prop_val)}")
        except Exception as e:
            raise
        #import ipdb; ipdb.set_trace()
         #   1+1
    return acc

with open("auto_campaign.json", "w") as f:
    json.dump(get_schema({}, campaign.Campaign()._pb), f)
with open("auto_ad.json", "w") as f:
    json.dump(get_schema({}, ad.Ad()._pb), f)
with open("auto_ad_group.json", "w") as f:
    json.dump(get_schema({}, ad_group.AdGroup()._pb), f)
with open("auto_account.json", "w") as f:
    json.dump(get_schema({}, customer.Customer()._pb), f)
print("Wrote schemas to local directory under auto_*.json, please review and manually set datetime formats.")
