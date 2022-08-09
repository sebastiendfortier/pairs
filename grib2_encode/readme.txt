The files on this disk provide ASCII data on all lightning events that occur
within the Canadian Lightning Network real-time delivery region.  This data
has been reprocessed prior to delivery to include late reports from sensors.
The files are named as follows: YYMMf.txt and YYMMs.txt; where YY is the
year of the data, MM is the month of the data, and 'f' or 's' denotes
whether the data is flash data or stroke data.

Within each file the data is divided into columns separated by a space.
The columns are as follows:

Stroke format:
Column 1:  Date, in the format YYYY-MM-DD
Column 2:  Time, in GMT, given to nanosecond resolution
Column 3:  Latitude, in signed decimal degrees
Column 4:  Longitude, in signed decimal degrees
Column 5:  Event strength, in kiloamperes
Column 6:  Chi squared value
Column 7:  Semi-major axis of the 50% confidence ellipse, in kilometers
Column 8:  Semi-minor axis of the 50% confidence ellipse, in kilometers
Column 9:  Angle of the confidence ellipse from North
Column 10: Cloud or ground status.  "C" indicates a cloud-cloud event,
     while "G" indicates a cloud-ground event.

Flash format:
Column 1:  Date, in the format YYYY-MM-DD
Column 2:  Time, in GMT, given to nanosecond resolution
Column 3:  Latitude, in signed decimal degrees
Column 4:  Longitude, in signed decimal degrees
Column 5:  Event strength, in kiloamperes
Column 6:  Chi squared value
Column 7:  Semi-major axis of the 50% confidence ellipse, in kilometers
Column 8:  Semi-minor axis of the 50% confidence ellipse, in kilometers
Column 9:  Angle of the confidence ellipse from North
Column 10: Multiplicity of the event
Column 11: Cloud or ground status.  "C" indicates a cloud-cloud event,
     while "G" indicates a cloud-ground event.

If there are any questions or problems, please call Global Atmospherics, Inc.
at 800-283-4557 or e-mail support@glatmos.com
