puts "==>> Ruby Process #{__FILE__} #{Process.pid} Starts To Process Zip File"

arg_arr = ARGV
arg_arr.each { |arg|
  puts "ARG: " << arg;
}

fn = arg_arr[0]
ln = arg_arr[1]
ap = arg_arr[2]
tm = arg_arr[3]
line_idx = 0

input_file = File.open(fn, 'r')

input_file.each_line { |line|
  line_idx += 1
  next if line_idx - 1 != ln.to_i
  $stdout.puts "sleep: #{ln.to_i % tm.to_i}"
  $stdout.flush
  sleep((ln.to_i % (tm.to_i + 1)) * 2);
  $stdout.puts "line: #{line_idx} zip: #{line}"
  $stdout.flush
}

puts "==>> Ruby Process #{__FILE__} #{Process.pid} Ends Processing Zip File"
