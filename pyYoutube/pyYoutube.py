from pytube import YouTube
# misc
import os
import shutil
import math
import datetime
# plots
import matplotlib.pyplot as plt
# %matplotlib inline
# image operation
# import cv2

# video = YouTube('https://www.youtube.com/watch?v=NqC_1GuY3dw')
# video = YouTube('https://www.youtube.com/watch?v=synr1ibQB_8')
# video = YouTube('https://www.youtube.com/watch?v=m1GxmBsxQ1g')
# video = YouTube('https://www.youtube.com/watch?v=54rRL-6aP7Y&list=RDm1GxmBsxQ1g&index=3')
video = YouTube('https://www.youtube.com/watch?v=RjHJSnhGnNg&list=RDm1GxmBsxQ1g&index=5')
# print(video.streams.filter(progressive=True, file_extension='mp4').order_by('resolution').desc().first())
# ... .download()
# for itm in video.streams.filter(type="audio", mime_type="audio/mp4"):
#     print(itm)

# for itm in video.streams.filter(file_extension="mp4"):
#     print(itm)
#
# for itm in video.streams.filter(progressive="True"):
#     print(itm)

print(video.streams.get_by_itag(140).default_filename)
print(video.streams.get_by_itag(137).default_filename)

# video.streams.get_by_itag(137).download("./downloads/" + video.streams.get_by_itag(140).default_filename.replace(".mp4", ".1080p.mp4"))
# video.streams.get_by_itag(140).download("./downloads/" + video.streams.get_by_itag(140).default_filename.replace(".mp4", ".128kbps.mp4"))
