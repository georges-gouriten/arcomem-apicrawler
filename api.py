import spiders
import apiblender.apiblender

blender = apiblender.apiblender.Blender()

myt = spiders.TwitterPages()
myt.set_keyword('lol')
myt.run(blender)
