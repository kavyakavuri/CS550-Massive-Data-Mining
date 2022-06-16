import os
import itertools
import sys

def get_items_greater_than_given_support(items_count_dict, support):
	freq = {}
	for item,count in items_count_dict.items():
		if count >= support:
			freq.update({item:count})

	return freq

def item_set_combinations_of_length_k_above_given_support(baskets, items_above_given_support, k):

	combinations = itertools.combinations(sorted(items_above_given_support),k)
	final_item_sets = {}

	for c in combinations:
		final_item_sets[c] = 0
	
	for basket in baskets:
		# filter out items which are above the given support threshold
		basket_items = [item for item in basket if item in items_above_given_support] 
		basket_items.sort()

		#generate all possible combinations of items of length k from basket_items
		item_sets = itertools.combinations(basket_items, k) 
		for item_set in item_sets:
			# if item_set in final_item_sets:
			final_item_sets[item_set] += 1

	return final_item_sets


def confidence(items_above_given_support, item_sets_above_given_support, k):

	conf = []
	k -= 1
	for tuple_, count in item_sets_above_given_support.items():

		if(k==1):
			key = (tuple_[0],tuple_[1])
			c = count/items_above_given_support[tuple_[0]]

			# conf(A->B)
			conf.append((key,c))					

			key = (tuple_[1],tuple_[0])
			c = count/items_above_given_support[tuple_[1]]

			# conf(B->A)
			conf.append((key,c))

		else: 	
			subsets = itertools.combinations(tuple_, k)
			for subset in subsets:
				if subset in items_above_given_support:
					key = tuple(list(subset) + list(set(tuple_)-set(subset)))
					c = count/items_above_given_support[subset]
					conf.append((key,c))
	return conf


if __name__ == "__main__":

	
	file_path = './data/browsing.txt'
	support = 100

	# read the data
	try:
		print("trying to open file at: ",file_path)
		f = open(file_path,"r")
		print("success")
	except IOError:
		print("File not found at ",file_path)
		sys.exit()


	support_values = {}
	baskets = []

	for line in f:
		basket = line.strip().split(' ')
		for item in basket:
			if item in support_values:
				support_values[item] += 1
			else:
				support_values[item] = 1
		baskets.append(set(basket))

	# generating items (of size 1) with support greater than given threshold
	items_above_given_support = get_items_greater_than_given_support(support_values, support) # this is a dict of type: {item : freq}

	#generating items sets' frequencies dict which are in items_above_given_support
	item_sets_size_2_above_threshold_support_freq_dict = item_set_combinations_of_length_k_above_given_support(baskets, items_above_given_support, 2)
	
	# generating item sets (of size 2) with support greater than given threshold
	item_sets_size_2_above_given_support = get_items_greater_than_given_support(item_sets_size_2_above_threshold_support_freq_dict, support) # this is a dict of type: {(item1,item2) : freq}

	# generating tuples of size 2 with their conf value
	confidence_couples = confidence(items_above_given_support, item_sets_size_2_above_given_support, 2)
	confidence_couples.sort(key = lambda x: x[1], reverse=True)

	print("\n\nTop 5 pairs with support =",support,"\n",'='*30)
	for couple in confidence_couples[:5]:
		print(couple[0][0]," => ", couple[0][1]," ", couple[1])
	print("-"*30)


	# Generating frequent triplets
	items_above_given_support = []
	# print(item_sets_size_2_above_given_support)
	for couple in item_sets_size_2_above_given_support: # iterating over keys of item_sets_size_2_above_given_support
		for item in couple:
			if item not in items_above_given_support:
				items_above_given_support.append(item)

	item_sets_size_3_above_threshold_support_freq_dict = item_set_combinations_of_length_k_above_given_support(baskets, items_above_given_support, 3)

	item_sets_size_3_above_given_support = get_items_greater_than_given_support(item_sets_size_3_above_threshold_support_freq_dict, support)
	confidence_triplets = confidence(item_sets_size_2_above_given_support, item_sets_size_3_above_given_support, 3)
	confidence_triplets.sort(key = lambda x:x[1], reverse=True)

	print("Top 5 triplets with support =", support,"\n",'='*40)
	for triplet in confidence_triplets[:5]:
		 print(triplet[0][0], " , ",triplet[0][1]," => ", triplet[0][2]," ", triplet[1])
	print('-'*40)
