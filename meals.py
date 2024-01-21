# this script is used to generate the meal service data
# meals are retrieved from themealdb.com and randomly assigned to airlines
# more API details: https://www.themealdb.com/api.php 
# before running the script, install dependencies:
# pip install requests
import requests
import csv
import time

# output file
f = open("meals.csv", "w")
f.write('meal_id|meal_name|meal_image|cat_name|tags|area|ingredient1|ingredient2|ingredient3|ingredient4|ingredient5|source|youtube\n')

random_url = 'https://www.themealdb.com/api/json/v1/1/random.php'
categories_url = 'https://www.themealdb.com/api/json/v1/1/categories.php'
random_by_category_url = 'https://www.themealdb.com/api/json/v1/1/filter.php?c='
lookup_url = 'https://www.themealdb.com/api/json/v1/1/lookup.php?i='


def make_request(url, parameter=None):
    
    response = None
    
    try:
        
        if parameter != None:
            url = url + parameter
        
        response = requests.get(url)
        
    except Exception as e:
        
        if 'Connection aborted' in str(e):
            time.sleep(60)
            response = requests.get(url)
            
    return response.json()


def main():
    
    num_records = 0 
    
    response = make_request(categories_url)    
    #print(response)
    
    categories = response['categories']
    
    for category in categories:
        cat_id = category['idCategory']
        cat_name = category['strCategory']
        cat_image = category['strCategoryThumb']
        cat_desc = category['strCategoryDescription']
        
        print(cat_name)

        response = make_request(random_by_category_url + cat_name)
        
        meals = response['meals']
        
        for meal in meals:
            
            meal_id = meal['idMeal']
            meal_name = meal['strMeal']
            meal_image = meal['strMealThumb']
            
            response = make_request(lookup_url + meal_id)
            meal_details = response['meals'][0]
            
            #print(meal_details)
            
            if 'strTags' in meal_details and meal_details['strTags'] != None:
                tags = meal_details['strTags']
            else:
                tags = ''
                
            if 'strArea' in meal_details and meal_details['strArea'] != None:
                area = meal_details['strArea']
            else:
                area = ''
             
            if 'strIngredient1' in meal_details and meal_details['strIngredient1'] != None:
                ingredient1 = meal_details['strIngredient1']
            else:
                ingredient1 = ''
            
            if 'strIngredient2' in meal_details and meal_details['strIngredient2'] != None:
                ingredient2 = meal_details['strIngredient2']
            else:
                ingredient2 = ''
                
            if 'strIngredient3' in meal_details and meal_details['strIngredient3'] != None:
                ingredient3 = meal_details['strIngredient3']
            else:
                ingredient3 = ''
                
            if 'strIngredient4' in meal_details and meal_details['strIngredient4'] != None:
                ingredient4 = meal_details['strIngredient4']
            else:
                ingredient4 = ''
            
            if 'strIngredient5' in meal_details and meal_details['strIngredient5'] != None:
                ingredient5 = meal_details['strIngredient5']
            else:
                ingredient5 = ''
            
            if 'strSource' in meal_details and meal_details['strSource'] != None:
                source = meal_details['strSource']
            else:
                source = ''
            
            if 'strYoutube' in meal_details and meal_details['strYoutube'] != None:
                youtube = meal_details['strYoutube']
            else:
                youtube = ''
            
            f.write(meal_id + '|' + meal_name + '|' + meal_image + '|' + cat_name + '|' + tags + '|' + area + '|') 
            f.write(ingredient1 + '|' + ingredient2 + '|' + ingredient3 + '|' + ingredient4 + '|' + ingredient5 + '|')
            f.write(source + '|' + youtube + '\n')
            
            num_records += 1
            print(num_records)
    
    return num_records    
    


if __name__=="__main__": 
    
    num_records = 0
    
    for i in range(200000):
        num_records += main()
        print('iteration {} complete. Num records {}'.format(i, num_records)) 
    
    f.close()



