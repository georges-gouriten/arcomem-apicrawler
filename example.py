import spiders
import output
import apiblender

blender = apiblender.Blender()

responses_handler = output.ResponsesHandler()

myt = spiders.TwitterPages(responses_handler)
myt.set_keywords(['bawz'])
myt.run(blender)

myt = spiders.YoutubePages(responses_handler)
myt.set_keywords(['bawz'])
myt.run(blender)

myt = spiders.GoogleplusPages(responses_handler)
myt.set_keywords(['barack'])
myt.run(blender)

myt = spiders.FlickrPages(responses_handler)
myt.set_keywords(['bawz'])
myt.run(blender)

myt = spiders.FacebookPages(responses_handler)
myt.set_keywords(['bawz'])
myt.run(blender)

