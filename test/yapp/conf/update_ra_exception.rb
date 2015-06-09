puts "==>> Ruby Process #{__FILE__} #{Process.pid} Starts"
sleep(3)
raise Exception.new("General Exception")
puts "==>> Ruby Process #{__FILE__} #{Process.pid} Ends"
