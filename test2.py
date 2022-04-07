import json
import ast
import pandas as pd

#the function that checks whether this block is in this grid
def whether_in_grid(point, grid):
    if point[0] <= grid[0][0]:
        return False
    elif point[1] > grid[0][1]:
        return False
    elif point[1] <= grid[1][1]:
        return False
    elif point[0] > grid[2][0]:
        return False
    else:
        return True

#this function need to return only the "rows" dict
#need to get ride of the first line because it is useless
#when read json line, get rid of ',' at the end and add r at begaining. use json.loads()
def read_grid_file(grid_file_path):
    with open(str(grid_file_path), 'r', encoding="utf-8") as g:
        grid_data = json.load(g)
    return grid_data

def read_whole_twitter_file(twitter_file_path):
    with open(str(twitter_file_path), 'r', encoding="utf-8") as f:
        data = json.load(f)
    return data


def process_grid(grid_data):
    grids = {}
    smallest_point = [grid_data["features"][0]['properties']['id'], grid_data["features"][0]['geometry']['coordinates'][0][1]]
    for i in grid_data["features"]:
        #print(i['geometry']['coordinates'][0])
        grids[str(i['geometry']['coordinates'][0][:4])] = i['properties']['id']
        if i['geometry']['coordinates'][0][:4][1] <= smallest_point:
            smallest_point = [i['properties']['id'],i['geometry']['coordinates'][0][1]]
    return grids, smallest_point

'''
def filter_empty_line(line_data):
    if line_data["doc"]["coordinates"] != None:
        return line_data
'''

#this function used for ditinguish which grid this line belongs to.
def distinguish_one_line(line_data, grids, smallest_point):
   if line_data["doc"]["coordinates"]['coordinates'] == smallest_point[1]:
       return line_data["doc"]["lang"], smallest_point[0]
   for i in list(grids):
       if whether_in_grid(line_data["doc"]["coordinates"]['coordinates'], ast.literal_eval(i)):
           return line_data["doc"]["lang"], grids[i]
   return False
   #
   #return language, grid_id

#function returns the grid name


def sum_the_output(result_dict, language, grid_id):
    if grid_id not in result_dict:
        result_dict[grid_id] = [1,{language:1}]
    else:
        [Total_Tweets, language_dict] = result_dict[grid_id]
        Total_Tweets = Total_Tweets + 1
        if language in language_dict:
            language_dict[language] = language_dict[language] + 1
        else:
            language_dict[language] = 1
        result_dict[grid_id] = [Total_Tweets, language_dict]
    return result_dict

def result_output(result_dict, language_dict):
    result_list = []
    for i in list(result_dict):
        top_10_dict = result_dict[i][1]
        #sort the dict
        top_10_dict = {k: v for k, v in sorted(top_10_dict.items(), key=lambda item: item[1], reverse=True)}
        top_10_list = []
        #get top 10 languages and tweets number
        if len(list(top_10_dict))>=10:
            for j in range(10):
                if list(top_10_dict)[j] in language_dict:
                    top_10_list.append(language_dict[list(top_10_dict)[j]] + '-' + str(top_10_dict[list(top_10_dict)[j]]))
                else:
                    top_10_list.append(list(top_10_dict)[j] + '-' + str(top_10_dict[list(top_10_dict)[j]]))
                #result_list.append([int(i), result_dict[i][0], len(list(result_dict[i][1])), ','.join(top_10_list)])
        else:
            for j in range(len(list(top_10_dict))):
                if list(top_10_dict)[j] in language_dict:
                    top_10_list.append(language_dict[list(top_10_dict)[j]] + '-' + str(top_10_dict[list(top_10_dict)[j]]))
                else:
                    top_10_list.append(list(top_10_dict)[j] + '-' + str(top_10_dict[list(top_10_dict)[j]]))
        result_list.append([int(i), result_dict[i][0], len(list(result_dict[i][1])), ','.join(top_10_list)])
    pd.set_option('display.colheader_justify', 'center')
    pd.set_option('max_colwidth', 200)
    df = pd.DataFrame(result_list, columns=["Cell", "#Total Tweets", "#Number of language used", "#Top 10 Languages & #Tweets"])
    # 行索引从1开始（原来的索引+1）
    df.index = df.index + 1
    print(df)

if __name__ == '__main__':
    language_dict = {'en': 'English', 'ar': 'Arabic', 'bn': 'Bengali', 'cs': 'Czech', 'da': 'Danish', 'de': 'German', 'el': 'Greek','es': 'Spanish', 'fa': 'Persian', 'fi': 'Finnish', 'fil': 'Filipino', 'fr': 'French', 'he': 'Hebrew', 'hi': 'Hindi', 'hu': 'Hungarian', 'id': 'Indonesian', 'it': 'Italian', 'ja': 'Japanese', 'ko': 'Korean', 'msa': 'Malay', 'nl': 'Dutch', 'no': 'Norwegian', 'pl': 'Polish', 'pt': 'Portuguese', 'ro': 'Romanian', 'ru': 'Russian', 'sv': 'Swedish', 'th': 'Thai', 'tr': 'Turkish', 'uk': 'Ukrainian', 'ur': 'Urdu', 'vi': 'Vietnamese', 'zh-cn': 'Chinese', 'zh-tw': 'Chinese'}
    grid_data = read_grid_file('./sydGrid.json')
    grids, smallest_point = process_grid(grid_data)
    result_dict={}
    
    with open('./bigTwitter.json', 'r', encoding="utf-8") as f:
        f.readline()
        n = 2
        while n < 4057524:
            n = n + 1
            line = f.readline().strip()
            if line:
                #print('line:',line)
                #print(''r' + line[:-2]:','r' + line[:-1])
                line_data = json.loads(line[:-1])
                if line_data["doc"]["coordinates"] != None and distinguish_one_line(line_data, grids, smallest_point) != False:
                    language, grid_id = distinguish_one_line(line_data, grids, smallest_point)
                    result_dict = sum_the_output(result_dict, language, grid_id)
                else:
                    continue
            else:
                #print("break")
                break
    #print('result_dict:', result_dict)
    result_output(result_dict, language_dict)
