#!/usr/bin/env python
# vim: tabstop=4:shiftwidth=4:smarttab:noexpandtab

# parallel_cp.py
# Copy a file (presumably from a network-mounted filesystem)
# in multiple parts simultaneously to minimize the slowing effects
# of latency or a shared connection (at the expense of increased disk IO).
# 
# You can accomplish the same thing (sans progressbar) using GNU's parallel and dd.

# parallel_cp.py requires progressbar (https://pypi.python.org/pypi/progressbar),
# which can be installed with pip.

import os
import sys
import argparse
import time
from multiprocessing import Process, Pipe, active_children
from progressbar import ProgressBar, Bar, Counter, ETA, Percentage

def main():
	args = get_arguments()

	# Set up children, then watch their progress
	source_size = os.path.getsize(args.source_file)
	print "Copying %s bytes using %s children" % (source_size, args.parts)
	children = spawn_children(args.source_file, args.destination_file, args.parts, source_size)
	show_progress(children, source_size)

	# Merge files and finish up
	print "Merging copied files"
	merge_files(args.destination_file, args.parts)
	print "All done!"
	sys.exit(0)

# Parse all arguments and return a namespace
def get_arguments(): # Parse our arguments
	parser = argparse.ArgumentParser(description='Copy a file in parts in parallel.')
	parser.add_argument('source_file', help="Path of source file.")
	parser.add_argument('destination_file', help="Path of destination file.")
	parser.add_argument('-p', '--parts', type=int, help="Number of parts to split the copy into.", default=5)
	args = parser.parse_args()

	# Check for a directory target and handle appropriately
	if os.path.isdir(args.destination_file):
		file_name = os.path.basename(args.source_file)
		args.destination_file = os.path.join(args.destination_file, file_name)
	return args

# Returns an array of Child objects with length <parts>
def spawn_children(source_file, destination_file, parts, source_size):
	children = [] # an array of Child objects
	for i in range(0,parts):
		pipes = Pipe() # Create pipe, one end for the parent and one end for this child
		p = Process(target=partial_copy, args=(source_file, destination_file, source_size, i, parts, pipes[0]))
		p.start()
		child = Child(p, pipes[1], get_copy_offsets(i, parts, source_size)[2])
		children.append(child)
	return children

# Creates a progress bar and updates it until all children exit
def show_progress(children, source_size):
	# Set up progressbar
	widgets = [Counter(), '/', str(source_size), "  (", Percentage(), ")  ", Bar(), '  ', ETA()]
	pbar = ProgressBar(widgets=widgets, maxval=source_size)
	pbar.start()

	# Update progressbar until copies are all complete
	while active_children():
		current_sum = 0
		for c in children:
			c.update()
			current_sum += c.bytes_copied
		pbar.update(current_sum)
		time.sleep(1)
	
	pbar.finish()

# Merges all the slices into dest_file
def merge_files(dest_file, parts):
	# Loop through all the files and append onto first
	with open("%s.0" % dest_file, 'ab') as out_fh:
		# Loop through each (other) slice, and append to the first
		for i in range(1,parts):
			current_file = "%s.%s" % (dest_file, i)
			with open(current_file) as in_fh:
				out_fh.write(in_fh.read())
			os.remove(current_file)

	# Rename the first slice, and we're all done
	os.rename("%s.0" % dest_file, dest_file)

# Copy a slice of a file, reporting process to parent when asked
# By default, this will copy in 1MB blocks (for status reporting)
def partial_copy(path_from, path_to, size_from, proc_num, total_procs, output, block_size=1048576):
	with open(path_from) as in_fh, open("%s.%s" % (path_to, proc_num), 'wb') as out_fh:
		# Figure out what part to copy
		start_pos, end_pos, read_len = get_copy_offsets(proc_num, total_procs, size_from)
		in_fh.seek(start_pos)
		bytes_read = 0

		# Until we've copied the whole slice, keep going
		while bytes_read < read_len:
			# Communication with parent; any input is a request for progress
			if output.poll():
				output.recv() # reset poll() to False
				output.send(bytes_read)

			# Calculate remaining bytes, then copy
			bytes_remaining = read_len - bytes_read
			if bytes_remaining > block_size: # copy a full block
				out_fh.write(in_fh.read(block_size))
				bytes_read += block_size
			else: # copy remaining data (< 1 block)
				out_fh.write(in_fh.read(bytes_remaining))
				bytes_read += bytes_remaining


# Returns (start/end/length) of the slice to be copied
def get_copy_offsets(proc_num, total_procs, filesize):
	getpos = lambda pnum, tprocs, fsize: int(float(pnum)/tprocs * fsize)

	start_pos = getpos(proc_num, total_procs, filesize)
	end_pos = getpos(proc_num+1, total_procs, filesize)
	read_len = end_pos - start_pos

	return (start_pos, end_pos, read_len)


# A class to wrap the concept of a child process
class Child (object):
	proc = None
	pipe = None
	bytes_copied = 0
	bytes_to_copy = 0

	def __init__(self, proc, pipe, bytes_to_copy):
		self.proc = proc
		self.pipe = pipe
		self.bytes_to_copy = bytes_to_copy

	def update(self):
		if not self.proc.is_alive():
			self.bytes_copied = self.bytes_to_copy
		else:
			if self.pipe.poll():
				self.bytes_copied = self.pipe.recv()
			self.pipe.send('') # request another update

# Our shim to invoke main()
if __name__ == '__main__':
	main()
