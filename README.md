# AWARE_earthquake_monitoring_system
# project duration
Sep 2014 ~ Sep 2015

# Motivation
	Earthquakes and specifically the building collapses that happen during the earthquake are a serious threat to many people living in high earthquake risk areas. 
	Collapsing of buildings is one of the primary causes of fatality in any earthquake. Although high-fidelity seismic sensors stations are deployed for early detection of earthquake tremors, 
	little effort has been put to detect collapse of buildings in real time. 
	The post-earthquake period of detecting collapsed buildings mostly involve image processing of aerial photographs. This method introduces a significant delay between the time of occurrence of the actual ‘collapsing’ event and the time it is detected, 
	thus not being suitable for real time collapse monitoring.


# How system works
	- Sensing a fall of a device or could be the user himself/herself
	- Communication between mobile client and backend server via UDP socket
	- Server calculates probability based on the algorithm (see more in our report)
	  and presumes an event of possible earthquake
	- Server broadcasts a warning message to all relevant clients of a detected earthquake
	- Mobile client of which has received warning message from the server provides useful information 
	  such as evaculation protocol or guidance to first aid ...
	  
	  
# Algorithm for detecting an earthquake event 

![alt tag](https://github.com/gowhd20/AWARE_earthquake_monitoring_system/blob/master/snippets/algorithm1.PNG)


# Time sync algorithm between server and clients

![alt tag](https://github.com/gowhd20/AWARE_earthquake_monitoring_system/blob/master/snippets/Cristian's_algorithm.JPG)


# Tools
	- Python with plugins
	- Java for android development
	- AWARE framework in which the mobile client was built upon
	
	
# AWARE framework for mobile client
http://www.awareframework.com/


# Demo video
https://youtu.be/eqjTYYYPrIs


# Contact information
	Aku Visuri (TA)
	aku.visuri@gmail.com

	Zeyun Zhu (PM)
	zeyunzhu@gmail.com
	
	Haejong dong
	s2haejong@gmail.com
	
	Perttu Pitkänen
	perttu.pitkanen@student.oulu.fi
	
	Pratyush Pandab
	pratyush.pandab@ee.oulu.fi
	pratyush.pp@gmail.com

