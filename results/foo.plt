set terminal png
set output "foo.png"
set title "TPS"
set style data histogram
set style histogram rowstacked
set style fill solid
set boxwidth 0.6 relative
plot for [COL=2:7] 'foo.out' using COL title columnheader;
