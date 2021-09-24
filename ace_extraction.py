import pandas as pd
from _ctypes import PyObj_FromPtr
import os
import re
import sys
import xml.etree.ElementTree as ET
import argparse
import json
from collections import Counter
from nltk.tokenize import LineTokenizer
import jieba

parser = argparse.ArgumentParser()
# add arguments here
parser.add_argument('--ace_path', type = str, default = './Chinese/', help = 'home directory of data files')
parser.add_argument('--output_path', type = str, default = './output/', help = 'output file path')

args = parser.parse_args()
# refer to arguments like args.ace_path, args.output_path

#sent_seg = LineTokenizer() # for English

class arg_ID:
    start=0
    end=0
    ID=0
'''class arg_ID_plus:
    start_1=0
    end_1=0
    start_2 = 0
    end_2 = 0
    ID=0'''

# events: [[event_type, [role1, role2, role2, role3]]]

# event_roles: {event_type:[role1, role2, role3]}


def get_index(filename,st,ed,dirflag,offset=0):
    if dirflag=='bn':
        return get_bn_index(filename,st,ed)
    if dirflag=='nw':
        return get_nw_index(filename,st,ed)
    if dirflag=='wl':
        return get_wl_index(filename,st,ed)

def get_bn_index(filename,st,ed):
    with open(filename,'r',encoding='utf-8') as f:
        lines = f.readlines()[3:]
    prefix = lines[0:4]
    prefix = "".join(prefix)
    lines = "".join(lines)
    lines = lines.replace('</TURN>\n<TURN>','\n')
    skip_num = len(prefix)
    prefix_len = len(prefix)
    for i in range(prefix_len,st):
        ch = lines[i]
        if ch in ['\n',' ']:
            skip_num += 1
    new_st = st - skip_num
    for i in range(st,ed):
        ch = lines[i]
        if ch in ['\n',' ']:
            skip_num += 1
    new_ed = ed - skip_num
    return new_st,new_ed

def get_nw_index(filename,st,ed):
    with open(filename,'r',encoding='utf-8') as f:
        lines = f.readlines()[3:]
    prefix = lines[0:6]
    prefix = "".join(prefix)
    lines = "".join(lines)
    skip_num = len(prefix)
    prefix_len = len(prefix)
    for i in range(prefix_len,st):
        ch = lines[i]
        if ch in ['\n',' ']:
            skip_num += 1
    new_st = st - skip_num
    for i in range(st,ed):
        ch = lines[i]
        if ch in ['\n',' ']:
            skip_num += 1
    new_ed = ed - skip_num
    return new_st,new_ed

def get_wl_index(filename,st,ed):
    with open(filename,'r',encoding='utf-8') as f:
        lines = f.readlines()
    while(lines[0] == '\n'):
        lines = lines[1:]
    lines = lines[3:]
    prefix = "".join((lines[0:9]))
    lines = "".join(lines[9:])
    for tag in ['</POST>', '<POST>','</POSTER>','</POSTDATE>']:
        lines = lines.replace(tag, "")
    lines = prefix + lines
    prefix_len = len(prefix)
    skip_num = prefix_len
    poster = []
    for i in range(prefix_len,st):
        ch = lines[i]
        if ch in ['\n',' ']:
            skip_num += 1
        elif lines[i:].startswith('&amp;'):
            skip_num += 4
        elif lines[i:].startswith('&#8226;'):
            skip_num += 6
        elif lines[i:].startswith('<POSTER>'):
            cnt = 0
            if filename.split('/')[-1].startswith('LANGLANGGARGEN') \
                    or filename.split('/')[-1].startswith('GLOVEBX')\
                    or filename.split('/')[-1].startswith('SHIHUA'):
                skip_num += 1
            skip_num -= 5
            for j in lines[i+8:]:
                cnt += 1
                if j != '\n':
                    skip_num += 1
                else:
                    skip_num += 1
                    break
            poster.append(lines[i+8:i+7+cnt].strip())
            lines=lines[0:i] + lines[i+8:]
            i += (cnt-1)
        elif lines[i:].startswith('<POSTDATE>'):
            cnt = 0
            for j in lines[i+10:]:
                cnt += 1
                if j != '\n':
                    skip_num += 1
                else:
                    skip_num += 1
                    break
            poster.append(lines[i + 10:i + 9 + cnt].strip())
            lines = lines[0:i] + lines[i+10:]
            i += (cnt-1)
    new_st = st - skip_num
    for i in range(st,ed):
        ch = lines[i]
        if ch in ['\n',' ']:
            skip_num += 1
        elif lines[i:].startswith('&amp;'):
            skip_num += 4
        elif lines[i:].startswith('&#8226;'):
            skip_num += 6
    new_ed = ed - skip_num
    for item in poster:
        if lines[st:ed] in item:
            return -1,-1
    return new_st,new_ed

def get_offset(filename,dirflag):
    if dirflag=='bn':
        offset = 0
        if filename.split('/')[-1] in ['CBS20001101.1000.0000.sgm',
                                       'CTV20001003.1330.0000.sgm']:
            offset = -28
        if filename.split('/')[-1] in ['CBS20001117.1000.0341.sgm',
                                       'CBS20001118.1000.0340.sgm',
                                       'CBS20001214.1000.1127.sgm',
                                       'CBS20001216.1000.0355.sgm']:
            offset = -29
        if filename.split('/')[-1] in ['CBS20001203.1000.0378.sgm',
                                       'VOM20001008.1800.0011.sgm',
                                       'VOM20001231.0700.0015.sgm']:
            offset = 4
    if dirflag == 'nw':
        offset = 15
    if dirflag == 'wl':
        offset = 62
        if filename.split('/')[-1] in ['DAVYZW_20050201.1538.sgm'] \
                or filename.split('/')[-1].startswith('GLOVEBX'):
            offset = 61
        if filename.split('/')[-1].startswith('LANGLANGGARGEN'):
            offset = 54
        if filename.split('/')[-1].startswith('LIUYIFENG'):
            offset = 59
        if filename.split('/')[-1].startswith('NJWSL'):
            offset = 63
        if filename.split('/')[-1].startswith('SHIHUA'):
            offset = 62
    return offset

def extract_events(document,filename,dirflag):
    events = []
    for node_event in document.iter('event'):
        event = []
        event_type = node_event.attrib.get('TYPE')+"."+node_event.attrib.get('SUBTYPE')
        event_ID = node_event.attrib.get('ID')

        event_argument = []
        for child_event in node_event.iter('event_argument'):
            event_argument.append([child_event.attrib['REFID'],child_event.attrib['ROLE']])

        for node_event_mention in node_event.iter('event_mention'):
            event_mention_ID=node_event_mention.get('ID')
            newtrigger_text=""
            anchor = list(node_event_mention.iter('anchor'))[0] # one anchor for one event mention
            trigger_charseq = list(anchor.iter('charseq'))[0] # only one charseq child
            newtrigger_text = trigger_charseq.text.replace(" ","")
            newtrigger_text = newtrigger_text.replace("\n","")
            st = trigger_charseq.get("START")
            ed = trigger_charseq.get("END")
            st = int(st)
            ed = int(ed)
            offset = get_offset(filename, dirflag)
            st += offset
            ed += offset
            new_st, new_ed = get_index(filename, st + 1, ed + 2, dirflag)
            assert(new_st >= 0)
            if (raw_text[new_st:new_ed] != newtrigger_text):
                print('------------- event')
                print(filename)
                print(raw_text)
                print(st, ed)
                print(new_st, new_ed)
                print(raw_text[new_st:new_ed])
                print(newtrigger_text)
                input()

            argument_inmention =[]
            for node_event_mention_argument in node_event_mention.iter('event_mention_argument'):
                argument_inmention.append([node_event_mention_argument.attrib['REFID'], node_event_mention_argument.attrib['ROLE']])
            event.append([event_mention_ID, event_type, new_st, new_ed,newtrigger_text, argument_inmention, event_argument])

        events.append(event)
        # print(events)
    return events

def extract_entities(document,filename,dirflag):
    entities=[]
    st=0
    ed=0
    for child_of_document in document.iter('entity'): # iterate each entity child
        entity=[]
        entity_type = child_of_document.attrib.get('TYPE')+"."+child_of_document.attrib.get('SUBTYPE')
        for child_entity in child_of_document.iter('entity_mention'): # iterate each entity_mention child
            newentity_text=""
            entity_mention_ID = child_entity.get('ID')

            head = list(child_entity.iter('head'))[0] # one head for one entity mention
            entity_charseq = list(head.iter('charseq'))[0] # only one charseq child

            st = entity_charseq.get("START")
            ed = entity_charseq.get("END")
            st = int(st)
            ed = int(ed)
            offset=get_offset(filename,dirflag)
            st += offset
            ed += offset

            newentity_text = entity_charseq.text.replace(" ", "")
            newentity_text = newentity_text.replace("\n","")

            new_st,new_ed = get_index(filename,st+1,ed+2,dirflag)
            if new_st<0:
                continue

            assert(new_st >= 0)
            if(raw_text[new_st:new_ed]!=newentity_text):
                print('------------- entity')
                print(filename)
                print(raw_text)
                print(st, ed)
                print(new_st , new_ed)
                print(raw_text[new_st:new_ed])
                print(newentity_text)
                input()
            entity.append([entity_mention_ID,entity_type,new_st, new_ed,newentity_text])
        entities.append(entity)
        # print(entity)
        # input()
    return entities

def extract_times(document,filename,dirflag):
    times = []
    for node_time in document.iter('timex2'):
        entity = []
        entity_type = "TIME"
        for node_time_mention in node_time.iter('timex2_mention'):
            newentity_text = ""
            entity_mention_ID = node_time_mention.get('ID')

            extent = list(node_time_mention.iter('extent'))[0] # one extent for one time mention
            entity_charseq = list(extent.iter('charseq'))[0] # only one charseq child
            newentity_text = entity_charseq.text.replace(" ", "")
            newentity_text = newentity_text.replace("\n", "")
            st = entity_charseq.get("START")
            ed = entity_charseq.get("END")
            st = int(st)
            ed = int(ed)
            offset = get_offset(filename, dirflag)
            st += offset
            ed += offset
            new_st,new_ed = get_index(filename,st+1,ed+2,dirflag)
            if(new_st <0) or (raw_text[new_st:new_ed]!=newentity_text):
                print('------------- time')
                print(filename)
                print(raw_text)
                print(st, ed)
                print(new_st , new_ed)
                print(raw_text[new_st:new_ed])
                print(newentity_text)
                #input()
            if new_st<0:
                continue
            entity.append([entity_mention_ID, entity_type, new_st,new_ed, newentity_text])
        times.append(entity)
    return times

def extract_values(document,filename,dirflag):
    values = []
    for node_value in document.iter('value'):
        entity = []
        entity_type = node_value.attrib.get('TYPE')
        for node_value_mention in node_value.iter('value_mention'):
            newentity_text = ""
            entity_mention_ID = node_value_mention.get('ID')
            extent = list(node_value_mention.iter('extent'))[0] # one extent for one value mention
            entity_charseq = list(extent.iter('charseq'))[0] # only one charseq child
            newentity_text = entity_charseq.text.replace(" ", "")
            newentity_text = newentity_text.replace("\n", "")
            st = entity_charseq.get("START")
            ed = entity_charseq.get("END")
            st = int(st)
            ed = int(ed)
            offset = get_offset(filename, dirflag)
            st += offset
            ed += offset
            new_st, new_ed = get_index(filename, st + 1, ed + 2, dirflag)

            if (raw_text[new_st:new_ed]!=newentity_text):
                print('------------- value')
                print(filename)
                print(raw_text)
                print(st, ed)
                print(new_st , new_ed)
                print(raw_text[new_st:new_ed])
                print(newentity_text)
                input()
            if new_st<0:
                print(4)
                continue
            entity.append([entity_mention_ID, entity_type, new_st, new_ed, newentity_text])
        values.append(entity)
    return values

def extract_content(filename, dirflag):
    sgm_tree = ET.parse(filename)
    body = sgm_tree.find('BODY')
    text = body.find('TEXT')
    raw_text = ""
    if dirflag == "bn":
        for TURN in text:
            turn = TURN.text.replace(" ","")
            turn = turn.replace("\n","")
            sentence = []
            for i,ch in enumerate(turn):
                if ch == '\n':
                    continue
                raw_text += ch
    elif dirflag == "nw":
        text = text.text
        text = text.replace(" ","")
        text = text.replace("\n\n", "<TURN>")
        text = text.replace("\n","")
        text = text.replace("<TURN>","\n")
        turns = text.split("\n")
        sentence = []
        for turn in turns:
            for i,ch in enumerate(turn):
                raw_text += ch
    elif dirflag == "wl":
        for POST in text:
            text = POST.find("POSTDATE").tail
            text = text.replace(" ", "")
            text = text.replace("\n\n", "<TURN>")
            text = text.replace("\n", "")
            text = text.replace("<TURN>", "\n")
            turns = text.split("\n")
            sentence = []
            for turn in turns:
                for i,ch in enumerate(turn):
                    raw_text += ch
    sentences = splitsentence(raw_text)
    #sentences = [list(jieba.cut(sent)) for sent in sentences]
    #print(sentences)
    return sentences,raw_text

resentencesp = re.compile('([﹒﹔﹖﹗．；。！？]["’”」』]{0,2}|：(?=["‘“「『]{1,2}|$))')
def splitsentence(sentence):
    s = sentence
    slist = []
    for i in resentencesp.split(s):
        if resentencesp.match(i) and slist:
            slist[-1] += i
        elif i:
            slist.append(i)
    return slist

if __name__ == '__main__':
    if not os.path.exists(args.output_path):
        os.mkdir(args.output_path)
    output_file = open(args.output_path + "ace_cn_full.json", "w",encoding='utf-8')
    print(output_file)

    event_records = []
    for dir_flag in ["bn", "nw", "wl"]:
        dir_path = os.path.join(args.ace_path, dir_flag+"/adj/")
        filename_list = [item[:-4] for item in os.listdir(dir_path) if item.endswith(".sgm")]
        print("Processing directory ", dir_path, len(filename_list), "e.g.,", filename_list[:2])
        for filename in filename_list:
            sgm_path = os.path.join(dir_path, filename+'.sgm')
            xml_path = os.path.join(dir_path, filename+'.apf.xml')

            #从sgm文件获取句子
            curr_contents,raw_text = extract_content(sgm_path, dir_flag)
            # ann_path = os.path.join(dir_path, filename+'.ann')

            # print(xml_path)
            # 解析xml文件
            apf_tree = ET.parse(xml_path)
            apf_root = apf_tree.getroot()

            # 获取文件名
            document = apf_root.find('document')
            document_name = document.attrib
            curr_events = extract_events(document,sgm_path,dir_flag)
            curr_entities = extract_entities(document,sgm_path,dir_flag)
            curr_times = extract_times(document,sgm_path,dir_flag)
            curr_values = extract_values(document,sgm_path,dir_flag)
            curr_entities+=curr_values
            curr_entities+=curr_times

            #提取datetime
            datetime=re.findall(r'\d{8}', filename)
            new_datetime=""
            pos=0
            for word in datetime:
                year=""
                month=""
                day=""
                for ch in word:
                    pos += 1
                    if pos > 6:
                        day += ch
                    elif pos >4:
                        month += ch
                    else: year += ch
            new_datetime = year+"/"+month+"/"+day

            # store contents and events, entities, times from one document to one json string
            output_json = dict()
            output_json["filename"] = filename
            output_json["doc_type"] = dir_flag
            output_json["datetime"] = new_datetime
            output_json["raw_text"] = raw_text
            output_json["sentences"] = curr_contents
            output_json["entities"] = curr_entities
            output_json["events"] = curr_events

            #output_file.write(json.dumps(output_json,ensure_ascii=False,indent=4))
            output_file.write(json.dumps(output_json,ensure_ascii=False)+"\n")
    output_file.close()
