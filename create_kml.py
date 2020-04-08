import simplekml

kml = simplekml.Kml()

with open('kmeans.res', 'r') as f:
    for i, line in enumerate(f):
        lon = float(line.split(" ")[0][1:])
        lat = float(line.split(" ")[2].strip("\n")[:-1])
        kml.newpoint(name="point " + str(i + 1), coords=[(lon, lat)])

kml.save("kmeans.kml")
