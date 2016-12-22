import threading
import myExtractor

queries= ['Donald Trump', 'Justin Trudeau']
threads = []
for i in queries:
    t = threading.Thread(target=myExtractor.extract, args=(i,))
    threads.append(t)
    t.start()



