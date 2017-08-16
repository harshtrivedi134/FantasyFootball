#!/usr/bin/python

import sys
from backport_collections import Counter

#need a locale to interpret number's formats properly
import locale
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

field = None

#each line will be the values for a particular field, extract the field name then loop through the values
for line in sys.stdin:
    split_line = line.split()
    prev_field = field
    field = split_line[0]
    if field == 'player_playerid':
        field = 'player_player_id'
    if field == 'playername':
        field = 'player_name'
    values = split_line[1:]
    
    if field != prev_field:
        if prev_field != None:
            print("Field: {0}".format(prev_field))
            print("Max Length: {0:d}".format(max_length))
            print("Max Numeric Value: {0}".format(max_num_val))
            print("Min Numeric Value: {0}".format(min_num_val))
            print("Average Value: {0:f}".format(running_nz_avg))
            print("Most Common Values: ")
            for example in cnt.most_common(10):
                 print("\t{0:20}: {1:>8d}".format(example[0], example[1]))

            print("\n")

        #set the initial values on several logical hypothesis to try to disprove with each value
        always_blank = True
        never_blank = True
        always_int = True
        always_int_or_blank = True
        always_float = True
        always_float_or_blank = True
        always_number = True
        always_number_or_blank = True
        never_number = True
    
        #initialize some cumulative values
        max_length = 0
        count = 0
        num_count = 0
        blank_count = 0
        max_val = None
        min_val = None
        max_num_val = None
        min_num_val = None
        running_nz_avg = 0
        running_avg = 0
        cnt = Counter()
    
    for val in values:
        try:    
            if len(val) > 0:
                is_blank = False
                always_blank = False
            else:
                is_blank = True
                never_blank = False
                blank_count += 1
            
            try:
                intval = locale.atoi(val)
                is_int = True
            except:
                is_int = False
                always_int = False
                if len(val) > 0:
                    always_int_or_blank = False
            
            try:
                floatval = locale.atof(val)
                is_float = True
                num_count += 1
            except:
                is_float = False
                always_float = False
                if len(val) > 0:
                    always_float_or_blank = False
            
            if is_int != True and is_float != True:
                always_number = False
                if is_blank == False:
                    always_number_or_blank = False
            else:
                never_number = False
            
            if len(val) > max_length:
                max_length = len(val)
            
            count += 1
            
            #Integers also get the is_float set to True due to being valid floating point values, find the max and min numeric values
            if is_float:
                if floatval > max_num_val or max_num_val is None:
                    max_num_val = floatval
                if floatval < min_num_val or min_num_val is None:
                    min_num_val = floatval
                    
                #calculate the running averages for both all numbers encountered and all numbers treating blanks as zeros
                running_nz_avg = (running_nz_avg * (num_count-1) + floatval) / num_count
                running_avg = (running_avg * (num_count + blank_count - 1) + floatval) / (num_count + blank_count)
                
            if is_blank and running_avg != 0:
                running_avg = (running_avg * (num_count + blank_count - 1) + 0) / (num_count + blank_count)
    
            #find the max and min values including strings and numbers
            if val > max_val or max_val is None:
                max_val = val
            if val < min_val or min_val is None:
                min_val = val
            
            #keep a dictionary of counters to track values encountered to be able show common values for field, throw away less common values periodically if dictionary gets too large
            cnt[val] += 1
            if len(cnt) > 200:
                cutoff = cnt.most_common(100)[99][1]
                for item in cnt:
                    if cnt[item] < cutoff:
                        del cnt[item]
        except:
            pass
    
print("Field: {0}".format(field))
print("Max Length: {0:d}".format(max_length))

print("Max Numeric Value: {0}".format(max_num_val))
print("Min Numeric Value: {0}".format(min_num_val))
print("Average Value: {0:f}".format(running_nz_avg))
print("Most Common Values: ")
for example in cnt.most_common(10):
    print("\t{0:20}: {1:>8d}".format(example[0], example[1]))

print
    
