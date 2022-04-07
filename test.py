import json
import ast
import pandas as pd

#the function that check whether this block in this grid
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

def read_data(twitter_file_path, grid_file_path):
    #with open('./smallTwitter.json', 'r', encoding="utf-8") as f:
    with open(str(twitter_file_path), 'r', encoding="utf-8") as f:
        data = json.load(f)
    #with open('./sydGrid.json', 'r', encoding="utf-8") as g:
    with open(str(grid_file_path), 'r', encoding="utf-8") as g:
        grid_data = json.load(g)
    #return data, grid_data


    #language types
    language_dict = {'en': 'English', 'ar': 'Arabic', 'bn': 'Bengali', 'cs': 'Czech', 'da': 'Danish', 'de': 'German', 'el': 'Greek','es': 'Spanish', 'fa': 'Persian', 'fi': 'Finnish', 'fil': 'Filipino', 'fr': 'French', 'he': 'Hebrew', 'hi': 'Hindi', 'hu': 'Hungarian', 'id': 'Indonesian', 'it': 'Italian', 'ja': 'Japanese', 'ko': 'Korean', 'msa': 'Malay', 'nl': 'Dutch', 'no': 'Norwegian', 'pl': 'Polish', 'pt': 'Portuguese', 'ro': 'Romanian', 'ru': 'Russian', 'sv': 'Swedish', 'th': 'Thai', 'tr': 'Turkish', 'uk': 'Ukrainian', 'ur': 'Urdu', 'vi': 'Vietnamese', 'zh-cn': 'Chinese', 'zh-tw': 'Chinese'}

    #print(data, type(data))
    #print(data["rows"][0]["doc"]["coordinates"])


    #filter all the none type data
    list_a=[]
    for i in data["rows"]:
        if i["doc"]["coordinates"] != None:
            list_a.append(i)
        else:
            pass
    #print(list_a)
    #print(len(list_a))


    #append grid name with 4 points
    grids = {}
    for i in grid_data["features"]:
        #print(i['geometry']['coordinates'][0])
        grids[str(i['geometry']['coordinates'][0][:4])] = i['properties']['id']

    #print(grids)
    #print('test:',ast.literal_eval(str(i['geometry']['coordinates'][0])))




    #print the result
    result_list=[]
    pd.set_option('display.colheader_justify', 'center')
    pd.set_option('max_colwidth',100)
    #df = pd.DataFrame(data, columns=["Cell", "#Total Tweets", "#Number of language used", "#Top 10 Languages & #Tweets"])
    #print(' Cell     #Total Tweets  #Number of language used    #Top 10 Languages & #Tweets')

    #compute the result
    for j in list(grids):
        twitter_dict={}
        for k in list_a:
            if whether_in_grid(k["doc"]["coordinates"]['coordinates'], ast.literal_eval(j)) and k["doc"]["lang"] not in twitter_dict:
                twitter_dict[k["doc"]["lang"]] = 1
            elif whether_in_grid(k["doc"]["coordinates"]['coordinates'], ast.literal_eval(j)) and k["doc"]["lang"] in twitter_dict:
                number = twitter_dict[k["doc"]["lang"]]
                number = number + 1
                twitter_dict[k["doc"]["lang"]] = number
            else:
                pass
        top_10_dict = {k: v for k, v in sorted(twitter_dict.items(), key=lambda item: item[1])}
        top_10_list = []
        for (l,m) in list(top_10_dict.items()):
            if l in language_dict:
                top_10_list.append(str(language_dict[l]) + '-' + str(m))
            else:
                pass
        #print(grids[j], sum(twitter_dict.values()), len(list(twitter_dict)), ','.join(top_10_list))
        result_list.append([int(grids[j]), sum(twitter_dict.values()), len(list(twitter_dict)), ','.join(top_10_list)])
    df = pd.DataFrame(result_list, columns=["Cell", "#Total Tweets", "#Number of language used", "#Top 10 Languages & #Tweets"])
    # 行索引从1开始（原来的索引+1）
    df.index = df.index + 1
    print(df)

if __name__ == '__main__':
    #def main(twitter_file_path, grid_file_path):
        #read_data(twitter_file_path, grid_file_path)
    read_data('./smallTwitter.json','./sydGrid.json')

#main('./smallTwitter.json','./sydGrid.json')
