

data = readLines("feeder.log");
data = data[grep("Inserted observation [0-9]* in [0-9]*ms", data)];
data = sub("ms", "", sub(".*observation [0-9]* in ","", data));
data = as.numeric(data);
data = data[data<=1000]
#png("plot.png", width = 2560, height = 1440, units = "px");
ma = function(x, n=5) { filter(x,rep(1/n,n), sides=2); }
plot(data, ylab="time (ms)",xlab="observation");
lines(ma(data, 500), col="red");
#dev.off();
