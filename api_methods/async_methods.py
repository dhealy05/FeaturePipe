loop = asyncio.get_event_loop()

def query():

    async def get_feature(action, num_minutes):
        #action == AVG or action == STDDEV
        seconds = time.time()
        boundary = (seconds - (60*num_minutes))
        query = 'SELECT ' + action + '(price), symbol FROM pulsar."public/default".all_stocks WHERE time > ' + str(boundary) + ' GROUP BY symbol'
        cursor.execute(query)
        result = cursor.fetchall()
        print(result)
        #print cursor.fetchall()
        #send_message(result[0])

    async_tasks = []

    for feature in feature_set:
        action, num_minutes = feature.split("_")
        async_tasks.append(loop.create_task(get_feature(action, int(num_minutes))))

    loop.run_until_complete(asyncio.gather(*async_tasks))

query()
