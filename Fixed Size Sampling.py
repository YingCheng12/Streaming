
import tweepy
import random
from collections import Counter

class MyStreamListener(tweepy.StreamListener):


    reservoir = []
    # single_tag_list = []
    sequence_number = 0
    # count = {}
    def on_status(self, status):
        tag = status.entities['hashtags']
        # print(type(tag))
        if tag:
            self.sequence_number = self.sequence_number+1
            if len(self.reservoir)<100:
                # self.reservoir.append(tag)
                # self.reservoir[self.sequence_number] = []
                self.reservoir.append(tag)
                # for each_tag in tag:
                    # print(each_tag)
                    # self.single_tag_list.append(each_tag)


            else:
                prob = 100/self.sequence_number
                weight = [prob, 1-prob]
                choices = [True, False]
                result = random.choices(choices, weight)
                random_key = random.randint(0, 99)
                if result:
                    self.reservoir[random_key] = tag
        # print(self.reservoir)
            single_tag = []
            for i in self.reservoir:
                for j in i:
                    # print(j['text'])
                    single_tag.append(j['text'])

            dict_tag_count = Counter(single_tag)
            dict_count_tag = {}

            for i in dict_tag_count.keys():
                temp_value = i
                temp_key = dict_tag_count[i]
                if temp_key in dict_count_tag.keys():
                    dict_count_tag[temp_key].append(temp_value)
                else:
                    dict_count_tag[temp_key] = []
                    dict_count_tag[temp_key].append(temp_value)

            sorted_key = sorted(dict_count_tag.keys(), reverse=True)



            print("The number of tweets with tags from the beginning:", self.sequence_number)
            # temp_result = []
            if (len(sorted_key)<=3):
                for i in sorted_key:
                    for j in sorted(dict_count_tag[i]):
                        temp = [j,i]
                        print(j,i)
                        # print("\n")
                        # temp_result.append(temp)
                        # temp_result.append(sorted(dict_count_tag[i]))
                        # print(temp)
            else:
                for i in sorted_key[0:3]:
                    for j in sorted(dict_count_tag[i]):
                        temp = [j,i]
                        print(j, i)
                        # print("\n")
                        # temp_result.append(temp)

                        # temp_result.append(sorted(dict_count_tag[i]))
                        # print(temp)

            # print(temp_result)

            print()


auth = tweepy.OAuthHandler("ymCCTUrOtdAPAGpReLYkTu9Lu", "0ZIkgQKC3K0teiVweQ6GpqByqtaqWhRZIJtaTAqMbf1Jr5VRTE")
auth.set_access_token("3309149994-Q29ABxtwuKOcXjUCQ2as2k7gr0HEcAJlL67UxF6", "VZMVlpxQDZN9xydXAApK51SmOv5f4ofT2oCI8HDWsRDAU")
api = tweepy.API(auth)
myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)
myStream.sample()