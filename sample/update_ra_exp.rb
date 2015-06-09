puts "==>> Ruby Process #{__FILE__} #{Process.pid} Starts, Process From #{ARGV[0]} to #{ARGV[1]}, Anchor Pos: #{ARGV[2]}"
sleep(15);
i = 0
i = ARGV[2].to_i + 1 if (ARGV[2].to_i >= i)
while i < 11
  $stdout.puts "UPDATE_RA_CUR_ID_POS=#{i}"
  $stdout.flush
  b = 5 / 0 if ((i == 5))
  i += 1
end
 
puts "==>> Ruby Process #{__FILE__} #{Process.pid} Ends"
