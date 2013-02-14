#set terminal png
#set output "TITLE_psvm.png"
#set xdata time
#set timefmt "%Y-%m-%d %H:%M:%S"
#set format y "%.0f"
#set format x "%H:%M"
#set y2range [0:800]
#set y2tics 0,100
#set ytics nomirror 
#set xtics out nomirror
#set mxtics 3
#set tics scale 3,1
#set size ratio 0.5
#set title "TITLE"
#set xlabel "time"
#set ylabel "kiloBytes"
#set y2label "% CPU"
#plot "FILE" using 1:8 axis x1y2 ti "cpu" with lines linecolor rgb "blue", \
#"FILE" using 1:5 axis x1y1 ti "rss" with lines linecolor rgb "red", \
#"FILE" using 1:6 axis x1y1 ti "vsz" with lines linecolor rgb "green";


#
# Gnuplot version 4.1 demo of multiplot
# auto-layout capability
#
#
set terminal png
set output "plain.png"

set multiplot layout 3, 1 title "Plain Jane"
set tmargin 2
set title "Cassandra"

set xdata time
set timefmt "%Y-%m-%d %H:%M:%S"
set format y "%.0f"
set format x "%H:%M"
set y2range [0:800]
set y2tics 0,100
set ytics nomirror 
set xtics out nomirror
set mxtics 3
set tics scale 3,1
#set xlabel "time"
#set ylabel "kiloBytes"
#set y2label "% CPU"

unset key
plot 'plainCass.cpu' using 1:8 axis x1y2 ti "cpu" with lines linecolor rgb "blue", \
'' using 1:5 axis x1y1 ti "rss" with lines linecolor rgb "red", \
'' using 1:6 axis x1y1 ti "vsz" with lines linecolor rgb "green";

#
set title "Surefire"
unset key
plot 'plainSure.cpu' using 1:8 axis x1y2 ti "cpu" with lines linecolor rgb "blue", \
'' using 1:5 axis x1y1 ti "rss" with lines linecolor rgb "red", \
'' using 1:6 axis x1y1 ti "vsz" with lines linecolor rgb "green";

#
unset format
unset xdata
unset ylabel
unset ytics
unset y2tics
unset y2range
set title "TPS"
set style data histogram
set style histogram rowstacked
set style fill solid
set boxwidth 0.6 relative
plot for [COL=2:7] 'foo.out' using COL title columnheader;
unset multiplot
