import time
import calendar
import pandas as pd
import numpy as np

pd.options.mode.chained_assignment = None  # default='warn'

# mapping between cities and data file names
CITY_DATA = { 'chicago': './chicago.csv',
              'new york city': './new_york_city.csv',
              'washington': './washington.csv' }

# mapping between user input and city choice
city_dict = {0: 'all', 1: 'chicago', 2: 'new york city', 3: 'washington'}

# mapping between user input and month choice and vice versa
element = ['all']
monthlist = [calendar.month_name[x].lower() for x in range(1,7)]
values = element + monthlist
keys = [x for x in range(len(values))]
month_dict = dict(zip(keys, values))
month_dict_rev = dict(zip(values, keys))

# mapping between user input and day of week choice and vice versa
daylist = [calendar.day_name[x].lower() for x in range(7)]
values = element + daylist
keys = [x for x in range(len(values))]
day_dict = dict(zip(keys, values))
day_dict_rev = dict(zip(values, keys))

def get_filters():
    """
    Asks user to select a city, month, and day (by entering an integer key) to analyze.

    Returns:
        (str) city - name of the city to analyze, or "all" to get the joint data of the cities
        (str) month - name of the month to filter by, or "all" to apply no month filter
        (str) day - name of the day of week to filter by, or "all" to apply no day filter
    """

    print('\n')
    print('Hello! Let\'s explore some US bikeshare data!\n')
    print('\nNote: Data is available for the first six months of 2017 only.\n')


    # get user input for city (all, chicago, new york city, washington)
    while True:
        try:
            city_input = int(input('\nSelect one of more cities by entering a number\n0 - ALL\n1 - Chicago\n2 - New York City\n3 - Washington\n'))
        except:
            print("Sorry, not a valid input. Please try again with a single digit number")
            continue
        city_valid_choices = list(city_dict.keys())
        if not city_input in city_valid_choices:
            print("Sorry, not a valid input. Please try again")
            continue
        else:
            break

    # get user input for month (all, january, february etc.)
    while True:
        try:
            month_input = int(input('\nSelect your filter for month by entering a number\n0 - ALL\n1 - January\n2 - February\n3 - March\n4 - April\n5 - May\n6 - June\n'))
        except:
            print("Sorry, not a valid input. Please try again with a single digit number")
            continue
        month_valid_choices = list(month_dict.keys())
        if not month_input in month_valid_choices:
            print("Sorry, not a valid input. Please try again")
            continue
        else:
            break

    # get user input for day of week (all, monday, tuesday, etc.)
    while True:
        try:
            day_input = int(input('\nSelect your filter for day of the week by entering a number\n0 - ALL\n1 - Monday\n2 - Tuesday\n3 - Wednesday\n4 - Thursday\n5 - Friday\n6 - Saturday\n7 - Sunday\n'))
        except:
            print("Sorry, not a valid input. Please try again with a single digit number")
            continue
        day_valid_choices = list(day_dict.keys())
        if not day_input in day_valid_choices:
            print("Sorry, not a valid input. Please try again")
            continue
        else:
            break

    # preparing to return city, month, day in string format
    city = city_dict[city_input]
    month = month_dict[month_input]
    day = day_dict[day_input]
    print('-'*40)
    print("\nThanks for your inputs! You have made the following selection:\n" + "\nCity: " + city.title() + "\nMonth: " + month.title() + "\nDay: " + day.title())
    print('-'*40)
    return city, month, day

def streamline_df(raw, city, month, day, month_input, day_input):
    """Data cleansing and filtering based on initial user inputs will transform raw data into a more useful DataFrame"""

    raw['Start Time'] = pd.to_datetime(raw['Start Time'])
    raw['End Time'] = pd.to_datetime(raw['End Time'])

    # extract hour from the Start Time column to create an hour column
    raw['hour'] = raw['Start Time'].dt.hour

    # extract month from the Start Time column to create a month column
    raw['month'] = raw['Start Time'].dt.month
    month_input = month_dict_rev[month]
    if month_input > 0:
        raw = raw.loc[raw['month'] == month_input]

    # extract day from the Start Time column to create a day column
    raw['day'] = raw['Start Time'].dt.weekday + 1
    day_input = day_dict_rev[day]
    if day_input > 0:
        raw = raw.loc[raw['day'] == day_input]

    # as this particular city is missing two columns of data, adding them in order to streamline
    if city == "washington":
        raw['Gender'] = 'Unknown'
        raw['Birth Year'] = 'Unknown'

    lean_columns = raw[['Start Time', 'End Time', 'Trip Duration', 'Start Station', 'End Station', 'User Type', 'Gender', 'Birth Year', 'month', 'day', 'hour']]
    lean_columns = lean_columns.fillna({'Gender':'Unknown','Birth Year':'Unknown'})
    lean_columns['Birth Year'] = lean_columns['Birth Year'].astype(str)
    df = lean_columns.dropna(axis = 0)
    return df

def display_data(df):
    step = 5
    x = step
    while True:
        x += step
        # option for the user to see 5 more rows of data
        restart = input('\nWould you like to see 5 more rows? Enter yes or no.\n')
        if restart.lower() != 'yes':
            break
        else:
            print(df.iloc[:x])

def view_first_rows_option(df):
    """Displays the first rows of the selected data if the user chooses to do so."""


    display_q = input('\nWould you like to see the first 5 rows of data in addition to the stats? Enter yes or no. Or enter yess to also sort by Start Date\n')
    if display_q.lower() == 'yes':
        print('\nFirst rows of data:\n')
        print(df.head())
        display_data(df)
    elif display_q.lower() == 'yess':
        print('\nFirst rows of data sorted by Start Date:\n')
        sort = df.sort_values(df.columns[0], ascending = True)
        print(sort.head())
        display_data(sort)
    else:
        print('\nI will take that as a no.\n')

def load_data(city, month, day):
    """
    Loads data based on city selection and filters by month and day if applicable.

    Args:
        (str) city - name of the city to analyze, or 'all' to get the joint data
        (str) month - name of the month to filter by, or "all" to apply no month filter
        (str) day - name of the day of week to filter by, or "all" to apply no day filter
    Returns:
        df - Pandas DataFrame containing city (or all cities) data filtered by month and day
    """
    # utilizing globally defined mapping between user input and month/day of week choice
    month_input = month_dict_rev[month]
    day_input = day_dict_rev[day]

    # if a single city is selected
    if city.lower() != 'all':
        # loading the raw data
        raw = pd.read_csv(CITY_DATA[city])

        # streamlining raw data into usable df
        df = streamline_df(raw, city, month, day, month_input, day_input)

        # making sure that our dataframe is not empty
        assert len(df) != 0

        # asking for user input on whether to display the first few rows, and, if so, whether it should be sorted by start date
        view_first_rows_option(df)

    # else: ALL cities have been selected by the user
    else:
        frames = []
        for i in range(1,4):
            # looping through each city
            city = city_dict[i]

            # loading the raw data
            raw = pd.read_csv(CITY_DATA[city])

            # streamlining raw data into usable df
            df = streamline_df(raw, city, month, day, month_input, day_input)

            # appending dataframe for each city to the frames list
            frames.append(df)

        # concatenating to one dataframe and adding some keys for each city
        df = pd.concat(frames, keys=['chicago', 'new_york_city', 'washington'])

        # making sure that our dataframe is not empty
        assert len(df) != 0

        # asking for user input on whether to display the first few rows, and, if so, whether it should be sorted by start date
        view_first_rows_option(df)

    return df


def time_stats(df):
    """Displays statistics on the most frequent times of travel."""
    print('-'*40)
    print('\nCalculating The Most Frequent Times of Travel...\n')
    start_time = time.time()

    # assigning variable for total rides (to be used in following calculations of relative frequencies)
    tot_rides = len(df.index)
    print("Total rides based on your initial selection: " + str(tot_rides) + "\n")

    # display the most common month and its relative frequency
    popular_month_index = df['month'].mode()[0]
    popular_month = month_dict[popular_month_index].title()
    freq_pop_month = df['month'].value_counts().max()
    freq_pm_percent = freq_pop_month / tot_rides * 100
    freq_pm_percent = "{0:.1f}".format(freq_pm_percent)
    print('Most Common Month: ' + popular_month + " - with " + str(freq_pop_month) + " occurrences (" + str(freq_pm_percent) + " percent)")


    # display the most common day of week and its relative frequency
    popular_day_index = df['day'].mode()[0]
    popular_day = day_dict[popular_day_index]
    freq_pop_day = df['day'].value_counts().max()
    freq_pd_percent = freq_pop_day / tot_rides * 100
    freq_pd_percent = "{0:.1f}".format(freq_pd_percent)
    print('\nMost Common Day of the Week: ' + popular_day.title() + " - with " + str(freq_pop_day) + " occurrences (" + str(freq_pd_percent) + " percent)")

    # display the most common start hour and its relative frequency
    popular_hour = df['hour'].mode()[0]
    end_popular_hour = (popular_hour + 1) % 24
    freq_pop_hour = df['hour'].value_counts().max()
    freq_ph_percent = freq_pop_hour / tot_rides * 100
    freq_ph_percent = "{0:.1f}".format(freq_ph_percent)
    print('\nMost Common Start Hour: ' + str(popular_hour) + "-" + str(end_popular_hour) + " - with " + str(freq_pop_hour) + " occurrences (" + str(freq_ph_percent) + " percent)")

    print("\nThis took %s seconds." % (time.time() - start_time))
    print('-'*40)


def station_stats(df):
    """Displays statistics on the most popular stations and trip."""

    print('\nCalculating The Most Popular Stations and Trip...\n')
    start_time = time.time()

    # display most commonly used start station
    popular_start_st = df['Start Station'].mode()[0]
    print("\nThe most popular start station is:\n" + popular_start_st)
    # display most commonly used end station
    popular_end_st = df['End Station'].mode()[0]
    print("\nThe most popular end station is:\n" + popular_end_st)

    # display most frequent combination of start station and end station trip
    df['combined'] = df['Start Station'] + " - " + df['End Station']
    popular_comb_st = df['combined'].mode()[0]
    print("\nThe most popular trip is:\n" + popular_comb_st)

    print("\nThis took %s seconds." % (time.time() - start_time))
    print('-'*40)

def seconds_to_sentence(seconds):
    """Takes a number seconds and translates it to a natural language string that could contain minutes, hours, days and years."""
    if seconds >= 365*24*60*60:
        rest = seconds
        years = int(rest // (365*24*60*60))
        rest = int(rest % (365*24*60*60))
        days = rest // (24*60*60)
        rest = int(rest % (24*60*60))
        hours = rest // (60*60)
        rest = int(rest % (60*60))
        minutes = rest // 60
        seconds = int(rest % 60)
        sentence = "{} years {} days {} hours {} minutes {} seconds".format(years, days, hours, minutes, seconds)
    elif seconds >= 24*60*60:
        rest = seconds
        days = int(rest // (24*60*60))
        rest = int(rest % (24*60*60))
        hours = rest // (60*60)
        rest = int(rest % (60*60))
        minutes = rest // 60
        seconds = int(rest % 60)
        sentence = "{} days {} hours {} minutes {} seconds".format(days, hours, minutes, seconds)
    elif seconds >= 60*60:
        rest = seconds
        hours = int(rest // (60*60))
        rest = int(rest % (60*60))
        minutes = int(rest // 60)
        seconds = int(rest % 60)
        sentence = "{} hours {} minutes {} seconds".format(hours, minutes, seconds)
    elif seconds >= 60:
        rest = seconds
        minutes = int(rest // 60)
        seconds = int(rest % 60)
        sentence = "{} minutes {} seconds".format(minutes, seconds)
    else:
        sentence = "{} seconds".format(seconds)
    return sentence

def trip_duration_stats(df):
    """Displays statistics on the total and average trip duration."""

    print('\nCalculating Trip Duration...\n')
    start_time = time.time()

    # display total travel time
    tot_travel_time = df['Trip Duration'].sum()
    print("Total travel time: " + seconds_to_sentence(tot_travel_time))

    # display mean travel time
    mean_travel_time = df['Trip Duration'].mean()
    print("\nMean travel time: " + seconds_to_sentence(mean_travel_time))

    # display median travel time
    median_travel_time = df['Trip Duration'].median()
    print("\nMedian travel time: " + seconds_to_sentence(median_travel_time))

    print("\nThis took %s seconds.." % (time.time() - start_time))
    print('-'*40)


def user_stats(df):
    """Displays statistics on bikeshare users."""

    print('\nCalculating User Stats...\n')
    start_time = time.time()

    # Display counts of user types
    user_type_count = df['User Type'].value_counts().to_frame()
    print('Counts of user type:\n')
    print(user_type_count)

    # Display counts of gender
    gender_count = df['Gender'].value_counts().to_frame()
    print('\nCounts of gender:\n')
    print(gender_count)

    # Display most common year of birth
    most_common_year = df['Birth Year'].value_counts().index.tolist()

    # using if statement to handle cases where the most common year is 'Unknown'
    # but also for washington data it must allow to return 'Unknown' when that's all there is.
    if most_common_year[0] == 'Unknown' and len(most_common_year) > 1:
        print('\nMost common birth year among customers: ' + str(most_common_year[1]))
    else:
        print('\nMost common birth year among customers: ' + str(most_common_year[0]))

    print("\nThis took %s seconds." % (time.time() - start_time))
    print('-'*40)


def main():
    while True:
        city, month, day = get_filters()
        df = load_data(city, month, day)

        time_stats(df)
        station_stats(df)
        trip_duration_stats(df)
        user_stats(df)

        # option for the user to restart
        restart = input('\nWould you like to restart? Enter yes or no.\n')
        if restart.lower() != 'yes':
            break


if __name__ == "__main__":
	main()
