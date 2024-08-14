#!/usr/bin/env python

from __future__ import absolute_import, division, print_function, unicode_literals
import xml.etree.ElementTree as ET
from xml.dom import minidom
import json
import zlib
import gzip
import bz2
import sys
import os
import io

try:
    import lzma
except ImportError:
    try:
        from backports import lzma
    except ImportError:
        lzma = None

try:
    from io import StringIO
except ImportError:
    try:
        from cStringIO import StringIO
    except ImportError:
        from StringIO import StringIO

PY2 = sys.version_info[0] == 2

# Compatibility for different string types between Python 2 and 3
try:
    unicode_type = unicode
    str_type = basestring
except NameError:
    unicode_type = str
    str_type = str

__program_name__ = "LoveSoStrong";
__project__ = __program_name__;
__project_url__ = "https://repo.or.cz/LoveSoStrong.git";
__version_info__ = (0, 0, 1, "RC 1", 1);
__version_date_info__ = (2024, 8, 14, "RC 1", 1);
__version_date__ = str(__version_date_info__[0]) + "." + str(__version_date_info__[1]).zfill(2) + "." + str(__version_date_info__[2]).zfill(2);
__revision__ = __version_info__[3];
__revision_id__ = "$Id$";
if(__version_info__[4] is not None):
 __version_date_plusrc__ = __version_date__ + "-" + str(__version_date_info__[4]);
if(__version_info__[4] is None):
 __version_date_plusrc__ = __version_date__;
if(__version_info__[3] is not None):
 __version__ = str(__version_info__[0]) + "." + str(__version_info__[1]) + "." + str(__version_info__[2]) + " " + str(__version_info__[3]);
if(__version_info__[3] is None):
 __version__ = str(__version_info__[0]) + "." + str(__version_info__[1]) + "." + str(__version_info__[2]);

class ZlibFile:
    def __init__(self, file_path=None, fileobj=None, mode='rb', level=9, wbits=15, encoding=None, errors=None, newline=None):
        if file_path is None and fileobj is None:
            raise ValueError("Either file_path or fileobj must be provided")
        if file_path is not None and fileobj is not None:
            raise ValueError("Only one of file_path or fileobj should be provided")

        self.file_path = file_path
        self.fileobj = fileobj
        self.mode = mode
        self.level = level
        self.wbits = wbits
        self.encoding = encoding
        self.errors = errors
        self.newline = newline
        self._compressed_data = b''
        self._decompressed_data = b''
        self._position = 0
        self._text_mode = 't' in mode

        # Force binary mode for internal handling
        internal_mode = mode.replace('t', 'b')

        if 'w' in mode or 'a' in mode or 'x' in mode:
            self.file = open(file_path, internal_mode) if file_path else fileobj
            self._compressor = zlib.compressobj(level, zlib.DEFLATED, wbits)
        elif 'r' in mode:
            if file_path:
                if os.path.exists(file_path):
                    self.file = open(file_path, internal_mode)
                    self._load_file()
                else:
                    raise FileNotFoundError("No such file: '{}'".format(file_path))
            elif fileobj:
                self.file = fileobj
                self._load_file()
        else:
            raise ValueError("Mode should be 'rb' or 'wb'")

    def write(self, data):
        """Write data to the file, compressing it in the process."""
        if 'w' not in self.mode and 'a' not in self.mode and 'x' not in self.mode:
            raise IOError("File not open for writing")

        if self._text_mode and isinstance(data, str):
            data = data.encode(self.encoding or 'utf-8', errors=self.errors)

        compressed_data = self._compressor.compress(data)
        self.file.write(compressed_data)

    def close(self):
        """Close the file, writing any remaining compressed data."""
        if 'w' in self.mode or 'a' in self.mode or 'x' in self.mode:
            self.file.write(self._compressor.flush())
        self.file.close()

    def _load_file(self):
        """Load and decompress the file content."""
        self._compressed_data = self.file.read()
        self._decompressed_data = zlib.decompress(self._compressed_data, self.wbits)
        self.file.close()

    def read(self, size=-1):
        """Read and return the decompressed data."""
        if size == -1:
            size = len(self._decompressed_data) - self._position
        data = self._decompressed_data[self._position:self._position + size]
        self._position += size
        return data

    def readline(self):
        """Read and return a single line from the decompressed data."""
        newline_pos = self._decompressed_data.find(b'\n', self._position)
        if newline_pos == -1:
            return self.read()  # Read until the end of the data
        line = self._decompressed_data[self._position:newline_pos + 1]
        self._position = newline_pos + 1
        return line

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def open_compressed_file(filename):
    """ Open a file, trying various compression methods if available. """
    if filename.endswith('.gz'):
        return gzip.open(filename, 'rt', encoding='utf-8')
    elif filename.endswith('.bz2'):
        return bz2.open(filename, 'rt', encoding='utf-8')
    elif filename.endswith('.xz') or filename.endswith('.lzma'):
        if lzma:
            return lzma.open(filename, 'rt', encoding='utf-8')
        else:
            raise ImportError("lzma module is not available")
    elif filename.endswith('.zl') or filename.endswith('.zz'):
        return ZlibFile(file_path=filename, mode='rb')
    else:
        return io.open(filename, 'r', encoding='utf-8')

def save_compressed_file(data, filename):
    """ Save data to a file, using various compression methods if specified. """
    if filename.endswith('.gz'):
        with gzip.open(filename, 'wt', encoding='utf-8') as file:
            file.write(data)
    elif filename.endswith('.bz2'):
        with bz2.open(filename, 'wt', encoding='utf-8') as file:
            file.write(data)
    elif filename.endswith('.xz') or filename.endswith('.lzma'):
        if lzma:
            with lzma.open(filename, 'wt', encoding='utf-8') as file:
                file.write(data)
        else:
            raise ImportError("lzma module is not available")
    elif filename.endswith('.zl') or filename.endswith('.zz'):
        with ZlibFile(file_path=filename, mode='wb') as file:
            if isinstance(data, str):
                file.write(data.encode('utf-8'))
            else:
                file.write(data)
    else:
        with io.open(filename, 'w', encoding='utf-8') as file:
            file.write(data)

def parse_line(line):
    """ Parse a line in the format 'var: value' and return the key and value. """
    parts = line.split(":", 1)
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    return None, None

def validate_non_negative_integer(value, key, line_number):
    """ Utility to validate that a given value is a non-negative integer """
    try:
        int_value = int(value)
        if int_value < 0:
            raise ValueError("Negative value '{0}' for key '{1}' on line {2}".format(value, key, line_number))
        return int_value
    except ValueError as e:
        raise ValueError("Invalid integer '{0}' for key '{1}' on line {2}".format(value, key, line_number))

def parse_file(filename, validate_only=False, verbose=False):
    with open_compressed_file(filename) as file:
        lines = file.readlines()
    return parse_lines(lines, validate_only, verbose)

def parse_string(data, validate_only=False, verbose=False):
    lines = StringIO(data).readlines()
    return parse_lines(lines, validate_only, verbose)

def parse_lines(lines, validate_only=False, verbose=False):
    services = []
    current_service = None
    in_section = {
        'user_list': False,
        'message_list': False,
        'message_thread': False,
        'user_info': False,
        'message_post': False,
        'bio_body': False,
        'message_body': False,
        'comment_section': False,
        'include_service': False,
        'include_users': False,
        'include_messages': False,
        'category_list': False,
        'description_body': False,
        'include_categories': False,
        'categorization_list': False,
        'info_body': False,
        'poll_list': False,
        'poll_body': False,
    }
    include_files = []
    user_id = None
    current_bio = None
    current_message = None
    current_thread = None
    current_category = None
    current_info = None
    current_poll = None
    current_polls = []
    categorization_values = {'Categories': [], 'Forums': []}
    category_ids = {'Categories': set(), 'Forums': set()}
    post_id = 1

    def parse_include_files(file_list):
        included_services = []
        for include_file in file_list:
            included_services.extend(parse_file(include_file, validate_only, verbose))
        return included_services

    def parse_include_users(file_list):
        users = {}
        for include_file in file_list:
            included_users = parse_file(include_file, validate_only, verbose)
            for service in included_users:
                users.update(service['Users'])
        return users

    def parse_include_messages(file_list):
        messages = []
        for include_file in file_list:
            included_messages = parse_file(include_file, validate_only, verbose)
            for service in included_messages:
                messages.extend(service['MessageThreads'])
        return messages

    def parse_include_categories(file_list):
        categories = []
        for include_file in file_list:
            included_categories = parse_file(include_file, validate_only, verbose)
            for service in included_categories:
                categories.extend(service['Categories'])
        return categories

    try:
        for line_number, line in enumerate(lines, 1):
            line = line.strip()
            if line == "--- Include Service Start ---":
                in_section['include_service'] = True
                include_files = []
                if verbose:
                    print("Line {0}: {1} (Starting include service section)".format(line_number, line))
                continue
            elif line == "--- Include Service End ---":
                in_section['include_service'] = False
                if verbose:
                    print("Line {0}: {1} (Ending include service section)".format(line_number, line))
                services.extend(parse_include_files(include_files))
                continue
            elif in_section['include_service']:
                include_files.append(line)
                if verbose:
                    print("Line {0}: {1} (Including file for service)".format(line_number, line))
                continue
            elif line == "--- Include Users Start ---":
                in_section['include_users'] = True
                include_files = []
                if verbose:
                    print("Line {0}: {1} (Starting include users section)".format(line_number, line))
                continue
            elif line == "--- Include Users End ---":
                in_section['include_users'] = False
                if verbose:
                    print("Line {0}: {1} (Ending include users section)".format(line_number, line))
                if current_service:
                    current_service['Users'].update(parse_include_users(include_files))
                continue
            elif in_section['include_users']:
                include_files.append(line)
                if verbose:
                    print("Line {0}: {1} (Including file for users)".format(line_number, line))
                continue
            elif line == "--- Include Messages Start ---":
                in_section['include_messages'] = True
                include_files = []
                if verbose:
                    print("Line {0}: {1} (Starting include messages section)".format(line_number, line))
                continue
            elif line == "--- Include Messages End ---":
                in_section['include_messages'] = False
                if verbose:
                    print("Line {0}: {1} (Ending include messages section)".format(line_number, line))
                if current_service:
                    current_service['MessageThreads'].extend(parse_include_messages(include_files))
                continue
            elif in_section['include_messages']:
                include_files.append(line)
                if verbose:
                    print("Line {0}: {1} (Including file for messages)".format(line_number, line))
                continue
            elif line == "--- Include Categories Start ---":
                in_section['include_categories'] = True
                include_files = []
                if verbose:
                    print("Line {0}: {1} (Starting include categories section)".format(line_number, line))
                continue
            elif line == "--- Include Categories End ---":
                in_section['include_categories'] = False
                if verbose:
                    print("Line {0}: {1} (Ending include categories section)".format(line_number, line))
                if current_service:
                    current_service['Categories'].extend(parse_include_categories(include_files))
                    for category in current_service['Categories']:
                        kind_split = category.get('Kind', '').split(",")
                        category['Type'] = kind_split[0].strip() if len(kind_split) > 0 else ""
                        category['Level'] = kind_split[1].strip() if len(kind_split) > 1 else ""
                        category_ids[category['Type']].add(category['ID'])
                continue
            elif in_section['include_categories']:
                include_files.append(line)
                if verbose:
                    print("Line {0}: {1} (Including file for categories)".format(line_number, line))
                continue
            elif line == "--- Start Archive Service ---":
                current_service = {'Users': {}, 'MessageThreads': [], 'Categories': [], 'Interactions': [], 'Categorization': {}, 'Info': ''}
                if verbose:
                    print("Line {0}: {1} (Starting new archive service)".format(line_number, line))
                continue
            elif line == "--- End Archive Service ---":
                services.append(current_service)
                current_service = None
                if verbose:
                    print("Line {0}: {1} (Ending archive service)".format(line_number, line))
                continue
            elif line == "--- Start Comment Section ---":
                in_section['comment_section'] = True
                if verbose:
                    print("Line {0}: {1} (Starting comment section)".format(line_number, line))
                continue
            elif line == "--- End Comment Section ---":
                in_section['comment_section'] = False
                if verbose:
                    print("Line {0}: {1} (Ending comment section)".format(line_number, line))
                continue
            elif in_section['comment_section']:
                if verbose:
                    print("Line {0}: {1} (Comment)".format(line_number, line))
                continue
            elif line == "--- Start Category List ---":
                in_section['category_list'] = True
                current_category = {}
                if verbose:
                    print("Line {0}: {1} (Starting category list)".format(line_number, line))
                continue
            elif line == "--- End Category List ---":
                in_section['category_list'] = False
                if current_category:
                    kind_split = current_category.get('Kind', '').split(",")
                    current_category['Type'] = kind_split[0].strip() if len(kind_split) > 0 else ""
                    current_category['Level'] = kind_split[1].strip() if len(kind_split) > 1 else ""
                    if current_category['Type'] not in categorization_values:
                        raise ValueError("Invalid 'Type' value '{0}' on line {1}. Expected one of {2}.".format(current_category['Type'], line_number, categorization_values.keys()))
                    if current_category['InSub'] != 0 and current_category['InSub'] not in category_ids[current_category['Type']]:
                        raise ValueError("InSub value '{0}' on line {1} does not match any existing ID values.".format(current_category['InSub'], line_number))
                    current_service['Categories'].append(current_category)
                    category_ids[current_category['Type']].add(current_category['ID'])
                current_category = None
                if verbose:
                    print("Line {0}: {1} (Ending category list)".format(line_number, line))
                continue
            elif line == "--- Start Categorization List ---":
                in_section['categorization_list'] = True
                current_service['Categorization'] = {}
                if verbose:
                    print("Line {0}: {1} (Starting categorization list)".format(line_number, line))
                continue
            elif line == "--- End Categorization List ---":
                in_section['categorization_list'] = False
                if verbose:
                    print("Line {0}: {1} (Ending categorization list)".format(line_number, line))
                categorization_values = current_service['Categorization']
                continue
            elif line == "--- Start Info Body ---":
                in_section['info_body'] = True
                if current_service:
                    current_info = []
                    if verbose:
                        print("Line {0}: {1} (Starting info body)".format(line_number, line))
                continue
            elif line == "--- End Info Body ---":
                in_section['info_body'] = False
                if current_service and current_info is not None:
                    current_service['Info'] = "\n".join(current_info)
                    current_info = None
                    if verbose:
                        print("Line {0}: {1} (Ending info body)".format(line_number, line))
                continue
            elif in_section['info_body']:
                if current_service and current_info is not None:
                    current_info.append(line)
                if verbose:
                    print("Line {0}: {1}".format(line_number, line))
                continue
            elif line == "--- Start Poll List ---":
                in_section['poll_list'] = True
                current_polls = []
                if verbose:
                    print("Line {0}: {1} (Starting poll list)".format(line_number, line))
                continue
            elif line == "--- End Poll List ---":
                in_section['poll_list'] = False
                if current_message:
                    current_message['Polls'] = current_polls
                if verbose:
                    print("Line {0}: {1} (Ending poll list)".format(line_number, line))
                continue
            elif in_section['poll_list'] and line == "--- Start Poll Body ---":
                in_section['poll_body'] = True
                current_poll = {}
                if verbose:
                    print("Line {0}: {1} (Starting poll body)".format(line_number, line))
                continue
            elif in_section['poll_body'] and line == "--- End Poll Body ---":
                in_section['poll_body'] = False
                if current_poll is not None:
                    current_polls.append(current_poll)
                    current_poll = None
                if verbose:
                    print("Line {0}: {1} (Ending poll body)".format(line_number, line))
                continue
            elif in_section['poll_body']:
                key, value = parse_line(line)
                if key and current_poll is not None:
                    if key in ['Answers', 'Results', 'Percentage']:
                        current_poll[key] = [item.strip() for item in value.split(',')]
                    else:
                        current_poll[key] = value
                continue
            elif current_service is not None:
                key, value = parse_line(line)
                if key == "Entry":
                    current_service['Entry'] = validate_non_negative_integer(value, "Entry", line_number)
                elif key == "Service":
                    current_service['Service'] = value
                elif key == "Categories":
                    current_service['Categorization']['Categories'] = [category.strip() for category in value.split(",")]
                    if verbose:
                        print("Line {0}: Categories set to {1}".format(line_number, current_service['Categorization']['Categories']))
                elif key == "Forums":
                    current_service['Categorization']['Forums'] = [forum.strip() for forum in value.split(",")]
                    if verbose:
                        print("Line {0}: Forums set to {1}".format(line_number, current_service['Categorization']['Forums']))
                elif in_section['category_list']:
                    if key == "Kind":
                        current_category['Kind'] = value
                    elif key == "ID":
                        current_category['ID'] = validate_non_negative_integer(value, "ID", line_number)
                    elif key == "InSub":
                        current_category['InSub'] = validate_non_negative_integer(value, "InSub", line_number)
                    elif key == "Headline":
                        current_category['Headline'] = value
                    elif key == "Description":
                        current_category['Description'] = value
                elif line == "--- Start User List ---":
                    in_section['user_list'] = True
                    if verbose:
                        print("Line {0}: {1} (Starting user list)".format(line_number, line))
                    continue
                elif line == "--- End User List ---":
                    in_section['user_list'] = False
                    if verbose:
                        print("Line {0}: {1} (Ending user list)".format(line_number, line))
                    continue
                elif line == "--- Start User Info ---":
                    in_section['user_info'] = True
                    if verbose:
                        print("Line {0}: {1} (Starting user info)".format(line_number, line))
                    continue
                elif line == "--- End User Info ---":
                    in_section['user_info'] = False
                    user_id = None
                    if verbose:
                        print("Line {0}: {1} (Ending user info)".format(line_number, line))
                    continue
                elif line == "--- Start Message List ---":
                    in_section['message_list'] = True
                    if verbose:
                        print("Line {0}: {1} (Starting message list)".format(line_number, line))
                    continue
                elif line == "--- End Message List ---":
                    in_section['message_list'] = False
                    if verbose:
                        print("Line {0}: {1} (Ending message list)".format(line_number, line))
                    continue
                elif line == "--- Start Message Thread ---":
                    in_section['message_thread'] = True
                    current_thread = {'Title': '', 'Messages': []}
                    post_id = 1
                    if verbose:
                        print("Line {0}: {1} (Starting message thread)".format(line_number, line))
                    continue
                elif line == "--- End Message Thread ---":
                    in_section['message_thread'] = False
                    current_service['MessageThreads'].append(current_thread)
                    current_thread = None
                    if verbose:
                        print("Line {0}: {1} (Ending message thread)".format(line_number, line))
                    continue
                elif line == "--- Start Message Post ---":
                    in_section['message_post'] = True
                    current_message = {}
                    if verbose:
                        print("Line {0}: {1} (Starting message post)".format(line_number, line))
                    continue
                elif line == "--- End Message Post ---":
                    in_section['message_post'] = False
                    if current_message:
                        current_thread['Messages'].append(current_message)
                    current_message = None
                    if verbose:
                        print("Line {0}: {1} (Ending message post)".format(line_number, line))
                    continue
                elif in_section['message_list'] and key == "Interactions":
                    current_service['Interactions'] = [interaction.strip() for interaction in value.split(",")]
                    if verbose:
                        print("Line {0}: Interactions set to {1}".format(line_number, current_service['Interactions']))
                elif in_section['message_list'] and key == "Status":
                    current_service['Status'] = [status.strip() for status in value.split(",")]
                    if verbose:
                        print("Line {0}: Status set to {1}".format(line_number, current_service['Status']))
                elif key == "Info":
                    current_info = []
                    in_section['info_body'] = True
                    if verbose:
                        print("Line {0}: {1} (Starting info body)".format(line_number, line))
                elif in_section['user_list'] and in_section['user_info']:
                    if key == "User":
                        user_id = validate_non_negative_integer(value, "User", line_number)
                        current_service['Users'][user_id] = {'Bio': ""}
                        if verbose:
                            print("Line {0}: User ID set to {1}".format(line_number, user_id))
                    elif key == "Name":
                        if user_id is not None:
                            current_service['Users'][user_id]['Name'] = value
                            if verbose:
                                print("Line {0}: Name set to {1}".format(line_number, value))
                    elif key == "Handle":
                        if user_id is not None:
                            current_service['Users'][user_id]['Handle'] = value
                            if verbose:
                                print("Line {0}: Handle set to {1}".format(line_number, value))
                    elif key == "Location":
                        if user_id is not None:
                            current_service['Users'][user_id]['Location'] = value
                            if verbose:
                                print("Line {0}: Location set to {1}".format(line_number, value))
                    elif key == "Joined":
                        if user_id is not None:
                            current_service['Users'][user_id]['Joined'] = value
                            if verbose:
                                print("Line {0}: Joined date set to {1}".format(line_number, value))
                    elif key == "Birthday":
                        if user_id is not None:
                            current_service['Users'][user_id]['Birthday'] = value
                            if verbose:
                                print("Line {0}: Birthday set to {1}".format(line_number, value))
                    elif line == "--- Start Bio Body ---":
                        if user_id is not None:
                            current_bio = []
                            in_section['bio_body'] = True
                            if verbose:
                                print("Line {0}: Starting bio body".format(line_number))
                    elif line == "--- End Bio Body ---":
                        if user_id is not None and current_bio is not None:
                            current_service['Users'][user_id]['Bio'] = "\n".join(current_bio)
                            current_bio = None
                            in_section['bio_body'] = False
                            if verbose:
                                print("Line {0}: Ending bio body".format(line_number))
                    elif in_section['bio_body'] and current_bio is not None:
                        current_bio.append(line)
                        if verbose:
                            print("Line {0}: Adding to bio body: {1}".format(line_number, line))
                elif in_section['message_list'] and in_section['message_thread']:
                    if key == "Thread":
                        current_thread['Thread'] = validate_non_negative_integer(value, "Thread", line_number)
                        if verbose:
                            print("Line {0}: Thread ID set to {1}".format(line_number, value))
                    elif key == "Category":
                        current_thread['Category'] = [category.strip() for category in value.split(",")]
                        if verbose:
                            print("Line {0}: Category set to {1}".format(line_number, current_thread['Category']))
                    elif key == "Forum":
                        current_thread['Forum'] = [forum.strip() for forum in value.split(",")]
                        if verbose:
                            print("Line {0}: Forum set to {1}".format(line_number, current_thread['Forum']))
                    elif key == "Title":
                        current_thread['Title'] = value
                        if verbose:
                            print("Line {0}: Title set to {1}".format(line_number, value))
                    elif key == "Type":
                        current_thread['Type'] = value
                        if verbose:
                            print("Line {0}: Type set to {1}".format(line_number, value))
                    elif key == "State":
                        current_thread['State'] = value
                        if verbose:
                            print("Line {0}: State set to {1}".format(line_number, value))
                    elif key == "Author":
                        current_message['Author'] = value
                        if verbose:
                            print("Line {0}: Author set to {1}".format(line_number, value))
                    elif key == "Time":
                        current_message['Time'] = value
                        if verbose:
                            print("Line {0}: Time set to {1}".format(line_number, value))
                    elif key == "Date":
                        current_message['Date'] = value
                        if verbose:
                            print("Line {0}: Date set to {1}".format(line_number, value))
                    elif key == "SubType":
                        current_message['SubType'] = value
                        if verbose:
                            print("Line {0}: SubType set to {1}".format(line_number, value))
                    elif key == "Post":
                        post_value = validate_non_negative_integer(value, "Post", line_number)
                        current_message['Post'] = post_value
                        if 'post_ids' not in current_thread:
                            current_thread['post_ids'] = set()
                        current_thread['post_ids'].add(post_value)
                        if verbose:
                            print("Line {0}: Post ID set to {1}".format(line_number, post_value))
                    elif key == "Nested":
                        nested_value = validate_non_negative_integer(value, "Nested", line_number)
                        if nested_value != 0 and nested_value not in current_thread.get('post_ids', set()):
                            raise ValueError(
                                "Nested value '{0}' on line {1} does not match any existing Post values in the current thread. Existing Post IDs: {2}".format(
                                    nested_value, line_number, list(current_thread.get('post_ids', set())))
                            )
                        current_message['Nested'] = nested_value
                        if verbose:
                            print("Line {0}: Nested set to {1}".format(line_number, nested_value))
                    elif line == "--- Start Message Body ---":
                        if current_message is not None:
                            current_message['Message'] = []
                            in_section['message_body'] = True
                            if verbose:
                                print("Line {0}: Starting message body".format(line_number))
                    elif line == "--- End Message Body ---":
                        if current_message is not None and 'Message' in current_message:
                            current_message['Message'] = "\n".join(current_message['Message'])
                            in_section['message_body'] = False
                            if verbose:
                                print("Line {0}: Ending message body".format(line_number))
                    elif in_section['message_body'] and current_message is not None and 'Message' in current_message:
                        current_message['Message'].append(line)
                        if verbose:
                            print("Line {0}: Adding to message body: {1}".format(line_number, line))

        if validate_only:
            return True, "", ""

        return services

    except Exception as e:
        if validate_only:
            return False, "Error: {0}".format(str(e)), lines[line_number - 1]
        else:
            raise

def display_services(services):
    for service in services:
        print("Service Entry: {0}".format(service['Entry']))
        print("Service: {0}".format(service['Service']))
        
        if 'Info' in service and service['Info']:
            print("Info: {0}".format(service['Info'].strip().replace("\n", "\n      ")))
        
        print("Interactions: {0}".format(', '.join(service['Interactions'])))
        print("Status: {0}".format(', '.join(service.get('Status', []))))
        
        if 'Categorization' in service and service['Categorization']:
            for category_type, category_levels in service['Categorization'].items():
                print("{0}: {0}".format(category_type, ', '.join(category_levels)))
        
        print("Category List:")
        for category in service['Categories']:
            print("  Type: {0}, Level: {1}".format(category.get('Type', 'N/A'), category.get('Level', 'N/A')))
            print("  ID: {0}".format(category['ID']))
            print("  InSub: {0}".format(category['InSub']))
            print("  Headline: {0}".format(category['Headline']))
            print("  Description: {0}".format(category['Description'].strip().replace("\n", "\n    ")))
            print("")
        
        print("User List:")
        for user_id, user_info in service['Users'].items():
            print("  User ID: {0}".format(user_id))
            print("    Name: {0}".format(user_info['Name']))
            print("    Handle: {0}".format(user_info['Handle']))
            print("    Location: {0}".format(user_info.get('Location', 'N/A')))
            print("    Joined: {0}".format(user_info.get('Joined', 'N/A')))
            print("    Birthday: {0}".format(user_info.get('Birthday', 'N/A')))
            print("    Bio:")
            print("      {0}".format(user_info.get('Bio', '').strip().replace("\n", "\n      ")))
            print("")
        
        print("Message Threads:")
        for idx, thread in enumerate(service['MessageThreads']):
            print("  --- Message Thread {0} ---".format(idx + 1))
            if thread['Title']:
                print("    Title: {0}".format(thread['Title']))
            if 'Category' in thread:
                print("    Category: {0}".format(', '.join(thread['Category'])))
            if 'Forum' in thread:
                print("    Forum: {0}".format(', '.join(thread['Forum'])))
            if 'Type' in thread:
                print("    Type: {0}".format(thread['Type']))
            if 'State' in thread:
                print("    State: {0}".format(thread['State']))
            
            for message in thread['Messages']:
                print("    {0} ({1} on {2}): [{3}] Post ID: {4} Nested: {5}".format(
                    message['Author'], message['Time'], message['Date'],
                    message.get('SubType', 'Post' if message['Post'] == 1 or message['Nested'] == 0 else 'Reply'),
                    message['Post'], message['Nested']))
                
                # Indent each line of the message body but keep it at the same level
                print("      {0}".format(message['Message'].strip().replace("\n", "\n      ")))
                
                if 'Polls' in message and message['Polls']:
                    print("      Polls:")
                    for poll in message['Polls']:
                        print("        Poll {0}:".format(poll.get('Num', 'N/A')))
                        print("          Question: {0}".format(poll.get('Question', 'N/A')))
                        print("          Answers: {0}".format(", ".join(poll.get('Answers', []))))
                        print("          Results: {0}".format(", ".join(str(r) for r in poll.get('Results', []))))
                        print("          Percentage: {0}".format(", ".join("{:.2f}".format(float(p)) for p in poll.get('Percentage', []))))
                        print("          Votes: {0}".format(poll.get('Votes', 'N/A')))
            print("")

def save_services_to_file(services, filename, line_ending="lf"):
    """Save the services data structure to a file with optional compression based on file extension."""
    output = []

    for service in services:
        output.append("--- Start Archive Service ---")

        output.append("Entry: {0}".format(service.get('Entry', 'N/A')))
        output.append("Service: {0}".format(service.get('Service', 'N/A')))

        if 'Info' in service:
            output.append("Info: {0}".format(service.get('Info', '<No information provided>')))

        if 'Interactions' in service:
            output.append("Interactions: {0}".format(", ".join(service['Interactions'])))

        if 'Status' in service:
            output.append("Status: {0}".format(", ".join(service['Status'])))

        if 'Categories' in service and service['Categories']:
            output.append("Categories:")
            for category in service['Categories']:
                output.append("  Type: {0}, Level: {1}".format(category.get('Type', 'N/A'), category.get('Level', 'N/A')))
                output.append("  ID: {0}".format(category.get('ID', 'N/A')))
                output.append("  InSub: {0}".format(category.get('InSub', 'N/A')))
                output.append("  Headline: {0}".format(category.get('Headline', 'N/A')))
                output.append("  Description: {0}".format(category.get('Description', '')))

        if 'MessageThreads' in service and service['MessageThreads']:
            output.append("Message Threads:")
            for thread in service['MessageThreads']:
                output.append("  --- Start Message Thread ---")
                output.append("  Thread: {0}".format(thread.get('Thread', 'N/A')))
                output.append("  Title: {0}".format(thread.get('Title', 'N/A')))
                output.append("  Category: {0}".format(", ".join(thread.get('Category', []))))
                output.append("  Forum: {0}".format(", ".join(thread.get('Forum', []))))
                output.append("  Type: {0}".format(thread.get('Type', 'N/A')))
                output.append("  State: {0}".format(thread.get('State', 'N/A')))

                if 'Messages' in thread and thread['Messages']:
                    for message in thread['Messages']:
                        output.append("  --- Start Message Post ---")
                        output.append("  Author: {0}".format(message.get('Author', 'N/A')))
                        output.append("  Time: {0}".format(message.get('Time', 'N/A')))
                        output.append("  Date: {0}".format(message.get('Date', 'N/A')))
                        output.append("  SubType: {0}".format(message.get('SubType', 'N/A')))
                        output.append("  Post: {0}".format(message.get('Post', 'N/A')))
                        output.append("  Nested: {0}".format(message.get('Nested', 'N/A')))

                        if 'Message' in message:
                            output.append("  Message:")
                            output.append("    {0}".format(message['Message']))

                        if 'Polls' in message and message['Polls']:
                            output.append("  Polls:")
                            output.append("  --- Start Poll List ---")
                            for poll in message['Polls']:
                                output.append("  --- Start Poll Body ---")
                                output.append("  Num: {0}".format(poll.get('Num', 'N/A')))
                                output.append("  Question: {0}".format(poll.get('Question', 'N/A')))
                                output.append("  Answers: {0}".format(", ".join(poll.get('Answers', []))))
                                output.append("  Results: {0}".format(", ".join(str(r) for r in poll.get('Results', []))))
                                output.append("  Percentage: {0}".format(", ".join("{:.2f}".format(float(p)) for p in poll.get('Percentage', []))))
                                output.append("  Votes: {0}".format(poll.get('Votes', 'N/A')))
                                output.append("  --- End Poll Body ---")
                            output.append("  --- End Poll List ---")
                        output.append("  --- End Message Post ---")
                output.append("  --- End Message Thread ---")

        if 'Users' in service and service['Users']:
            output.append("User List:")
            for user_id, user in service['Users'].items():
                output.append("  User ID: {0}".format(user_id))
                output.append("    Name: {0}".format(user.get('Name', 'N/A')))
                output.append("    Handle: {0}".format(user.get('Handle', 'N/A')))
                output.append("    Location: {0}".format(user.get('Location', 'N/A')))
                output.append("    Joined: {0}".format(user.get('Joined', 'N/A')))
                output.append("    Birthday: {0}".format(user.get('Birthday', 'N/A')))
                output.append("    Bio:")
                output.append("      {0}".format(user.get('Bio', '').replace("\n", "\n      ")))

        output.append("--- End Archive Service ---")
        output.append("")

    # Join all output lines with the appropriate line ending
    data = "\n".join(output)

    # Save the data to the file with the appropriate compression
    save_compressed_file(data, filename)


def to_json(services):
    """ Convert the services data structure to JSON """
    return json.dumps(services, indent=2)

def from_json(json_str):
    """ Convert a JSON string back to the services data structure """
    return json.loads(json_str)

def load_from_json_file(json_filename):
    """ Load the services data structure from a JSON file """
    with open_compressed_file(json_filename) as file:
        return json.load(file)

def to_xml(services):
    """ Convert the services data structure to an XML string """
    root = ET.Element("Services")
    
    for service in services:
        service_elem = ET.SubElement(root, "Service")
        for key, value in service.items():
            if isinstance(value, list):
                list_elem = ET.SubElement(service_elem, key)
                for item in value:
                    if isinstance(item, dict):
                        item_elem = ET.SubElement(list_elem, key[:-1])  # singular form
                        for subkey, subvalue in item.items():
                            sub_elem = ET.SubElement(item_elem, subkey)
                            sub_elem.text = unicode_type(subvalue)
                    else:
                        item_elem = ET.SubElement(list_elem, key[:-1])
                        item_elem.text = unicode_type(item)
            elif isinstance(value, dict):
                dict_elem = ET.SubElement(service_elem, key)
                for subkey, subvalue in value.items():
                    sub_elem = ET.SubElement(dict_elem, subkey)
                    if isinstance(subvalue, list):
                        for sub_item in subvalue:
                            sub_item_elem = ET.SubElement(sub_elem, subkey[:-1])
                            sub_item_elem.text = unicode_type(sub_item)
                    else:
                        sub_elem.text = unicode_type(subvalue)
            else:
                elem = ET.SubElement(service_elem, key)
                elem.text = unicode_type(value)
    
    # Convert to string
    xml_str = ET.tostring(root, encoding='utf-8')
    if PY2:
        xml_str = xml_str.decode('utf-8')  # Convert bytes to str in Python 2
    # Make the XML string pretty
    xml_str = minidom.parseString(xml_str).toprettyxml(indent="  ")
    return xml_str

def from_xml(xml_str):
    """ Convert an XML string back to the services data structure """
    services = []
    root = ET.fromstring(xml_str)
    
    for service_elem in root.findall('Service'):
        service = {}
        for child in service_elem:
            if list(child):  # If there are nested elements
                if child.tag in service:
                    service[child.tag].append(parse_xml_element(child))
                else:
                    service[child.tag] = [parse_xml_element(child)]
            else:
                service[child.tag] = child.text
        services.append(service)
    
    return services

def parse_xml_element(element):
    """ Helper function to parse XML elements into a dictionary """
    result = {}
    for child in element:
        if list(child):
            result[child.tag] = parse_xml_element(child)
        else:
            result[child.tag] = child.text
    return result

def open_compressed_file(filename):
    """ Open a file, trying various compression methods if available. """
    if filename.endswith('.gz'):
        import gzip
        return gzip.open(filename, 'rt', encoding='utf-8') if not PY2 else gzip.open(filename, 'r')
    elif filename.endswith('.bz2'):
        import bz2
        return bz2.open(filename, 'rt', encoding='utf-8') if not PY2 else bz2.open(filename, 'r')
    elif filename.endswith('.xz') or filename.endswith('.lzma'):
        try:
            import lzma
        except ImportError:
            from backports import lzma
        return lzma.open(filename, 'rt', encoding='utf-8') if not PY2 else lzma.open(filename, 'r')
    else:
        return open(filename, 'r', encoding='utf-8') if not PY2 else open(filename, 'r')

def save_compressed_file(data, filename):
    """ Save data to a file, using various compression methods if specified. """
    if filename.endswith('.gz'):
        import gzip
        with gzip.open(filename, 'wt', encoding='utf-8') if not PY2 else gzip.open(filename, 'w') as file:
            if PY2:
                file.write(data.encode('utf-8'))
            else:
                file.write(data)
    elif filename.endswith('.bz2'):
        import bz2
        with bz2.open(filename, 'wt', encoding='utf-8') if not PY2 else bz2.open(filename, 'w') as file:
            if PY2:
                file.write(data.encode('utf-8'))
            else:
                file.write(data)
    elif filename.endswith('.xz') or filename.endswith('.lzma'):
        try:
            import lzma
        except ImportError:
            from backports import lzma
        with lzma.open(filename, 'wt', encoding='utf-8') if not PY2 else lzma.open(filename, 'w') as file:
            if PY2:
                file.write(data.encode('utf-8'))
            else:
                file.write(data)
    else:
        with open(filename, 'w', encoding='utf-8') if not PY2 else open(filename, 'w') as file:
            if PY2:
                file.write(data.encode('utf-8'))
            else:
                file.write(data)

def load_from_xml_file(xml_filename):
    """ Load the services data structure from an XML file """
    with open_compressed_file(xml_filename) as file:
        xml_str = file.read()
    return from_xml(xml_str)

def save_to_xml_file(services, xml_filename):
    """ Save the services data structure to an XML file """
    xml_str = to_xml(services)
    save_compressed_file(xml_str, xml_filename)

def save_to_json_file(services, json_filename):
    """ Save the services data structure to a JSON file """
    json_data = json.dumps(services, indent=2)
    save_compressed_file(json_data, json_filename)

def services_to_string(services):
    """Convert the services structure into a string format suitable for saving to a file."""
    output = []
    
    for service in services:
        output.append("--- Start Archive Service ---")
        
        output.append("Entry: {0}".format(service.get('Entry', 'N/A')))
        output.append("Service: {0}".format(service.get('Service', 'N/A')))
        
        if 'Info' in service:
            output.append("Info: {0}".format(service.get('Info', '<No information provided>')))
        
        if 'Interactions' in service:
            output.append("Interactions: {0}".format(", ".join(service['Interactions'])))
        
        if 'Status' in service:
            output.append("Status: {0}".format(", ".join(service['Status'])))
        
        if 'Categories' in service and service['Categories']:
            output.append("Categories:")
            for category in service['Categories']:
                output.append("  Type: {0}, Level: {1}".format(category.get('Type', 'N/A'), category.get('Level', 'N/A')))
                output.append("  ID: {0}".format(category.get('ID', 'N/A')))
                output.append("  InSub: {0}".format(category.get('InSub', 'N/A')))
                output.append("  Headline: {0}".format(category.get('Headline', 'N/A')))
                output.append("  Description: {0}".format(category.get('Description', '')))
        
        if 'MessageThreads' in service and service['MessageThreads']:
            output.append("Message Threads:")
            for thread in service['MessageThreads']:
                output.append("  --- Start Message Thread ---")
                output.append("  Thread: {0}".format(thread.get('Thread', 'N/A')))
                output.append("  Title: {0}".format(thread.get('Title', 'N/A')))
                output.append("  Category: {0}".format(", ".join(thread.get('Category', []))))
                output.append("  Forum: {0}".format(", ".join(thread.get('Forum', []))))
                output.append("  Type: {0}".format(thread.get('Type', 'N/A')))
                output.append("  State: {0}".format(thread.get('State', 'N/A')))
                
                if 'Messages' in thread and thread['Messages']:
                    for message in thread['Messages']:
                        output.append("  --- Start Message Post ---")
                        output.append("  Author: {0}".format(message.get('Author', 'N/A')))
                        output.append("  Time: {0}".format(message.get('Time', 'N/A')))
                        output.append("  Date: {0}".format(message.get('Date', 'N/A')))
                        output.append("  SubType: {0}".format(message.get('SubType', 'N/A')))
                        output.append("  Post: {0}".format(message.get('Post', 'N/A')))
                        output.append("  Nested: {0}".format(message.get('Nested', 'N/A')))
                        
                        if 'Message' in message:
                            output.append("  Message:")
                            output.append("    {0}".format(message['Message']))
                        
                        if 'Polls' in message and message['Polls']:
                            output.append("  Polls:")
                            output.append("  --- Start Poll List ---")
                            for poll in message['Polls']:
                                output.append("  --- Start Poll Body ---")
                                output.append("  Num: {0}".format(poll.get('Num', 'N/A')))
                                output.append("  Question: {0}".format(poll.get('Question', 'N/A')))
                                output.append("  Answers: {0}".format(", ".join(poll.get('Answers', []))))
                                output.append("  Results: {0}".format(", ".join(str(r) for r in poll.get('Results', []))))
                                output.append("  Percentage: {0}".format(", ".join("{:.2f}".format(float(p)) for p in poll.get('Percentage', []))))
                                output.append("  Votes: {0}".format(poll.get('Votes', 'N/A')))
                                output.append("  --- End Poll Body ---")
                            output.append("  --- End Poll List ---")
                        output.append("  --- End Message Post ---")
                output.append("  --- End Message Thread ---")
        
        if 'Users' in service and service['Users']:
            output.append("User List:")
            for user_id, user in service['Users'].items():
                output.append("  User ID: {0}".format(user_id))
                output.append("    Name: {0}".format(user.get('Name', 'N/A')))
                output.append("    Handle: {0}".format(user.get('Handle', 'N/A')))
                output.append("    Location: {0}".format(user.get('Location', 'N/A')))
                output.append("    Joined: {0}".format(user.get('Joined', 'N/A')))
                output.append("    Birthday: {0}".format(user.get('Birthday', 'N/A')))
                output.append("    Bio:")
                output.append("      {0}".format(user.get('Bio', '').replace("\n", "\n      ")))
        
        output.append("--- End Archive Service ---")
        output.append("")

    return "\n".join(output)
    
    line_sep = {"lf": "\n", "cr": "\r", "crlf": "\r\n"}
    return line_sep.get(line_ending, "\n").join(lines)

def save_services_to_file(services, filename, line_ending="lf"):
    """ Save the services data structure to a file in the original text format """
    data = services_to_string(services, line_ending)
    save_compressed_file(data, filename)

def init_empty_service(entry, service_name, info=''):
    """ Initialize an empty service structure """
    return {
        'Entry': entry,
        'Service': service_name,
        'Users': {},
        'MessageThreads': [],
        'Categories': [],
        'Interactions': [],
        'Categorization': {},
        'Info': info,
    }

def add_user(service, user_id, name, handle, location='', joined='', birthday='', bio=''):
    """ Add a user to the service """
    service['Users'][user_id] = {
        'Name': name,
        'Handle': handle,
        'Location': location,
        'Joined': joined,
        'Birthday': birthday,
        'Bio': bio
    }

def add_category(service, kind, category_type, category_level, category_id, insub, headline, description):
    category = {
        'Kind': "{0}, {1}".format(kind, category_level),
        'Type': category_type,
        'Level': category_level,
        'ID': category_id,
        'InSub': insub,
        'Headline': headline,
        'Description': description
    }
    service['Categories'].append(category)
    if category_type not in service['Categorization']:
        service['Categorization'][category_type] = []
    if category_level not in service['Categorization'][category_type]:
        service['Categorization'][category_type].append(category_level)
    if insub != 0:
        if not any(cat['ID'] == insub for cat in service['Categories']):
            raise ValueError("InSub value '{0}' does not match any existing ID in service.".format(insub))

def add_message_thread(service, thread_id, title='', category='', forum='', thread_type='', state=''):
    """ Add a message thread to the service """
    thread = {
        'Thread': thread_id,
        'Title': title,
        'Category': category.split(',') if category else [],
        'Forum': forum.split(',') if forum else [],
        'Type': thread_type,
        'State': state,
        'Messages': []
    }
    service['MessageThreads'].append(thread)

def add_message_post(service, thread_id, author, time, date, subtype, post_id, nested, message):
    thread = next((t for t in service['MessageThreads'] if t['Thread'] == thread_id), None)
    if thread is not None:
        new_post = {
            'Author': author,
            'Time': time,
            'Date': date,
            'SubType': subtype,
            'Post': post_id,
            'Nested': nested,
            'Message': message
        }
        thread['Messages'].append(new_post)
    else:
        raise ValueError("Thread ID {0} not found in service.".format(thread_id))

def add_poll(service, thread_id, post_id, poll_num, question, answers, results, percentages, votes):
    thread = next((t for t in service['MessageThreads'] if t['Thread'] == thread_id), None)
    if thread is not None:
        message = next((m for m in thread['Messages'] if m['Post'] == post_id), None)
        if message is not None:
            if 'Polls' not in message:
                message['Polls'] = []
            new_poll = {
                'Num': poll_num,
                'Question': question,
                'Answers': answers,
                'Results': results,
                'Percentage': percentages,
                'Votes': votes
            }
            message['Polls'].append(new_poll)
        else:
            raise ValueError("Post ID {0} not found in thread {1}.".format(post_id, thread_id))
    else:
        raise ValueError("Thread ID {0} not found in service.".format(thread_id))

def remove_user(service, user_id):
    if user_id in service['Users']:
        del service['Users'][user_id]
    else:
        raise ValueError("User ID {0} not found in service.".format(user_id))

def remove_category(service, category_id):
    category = next((c for c in service['Categories'] if c['ID'] == category_id), None)
    if category:
        service['Categories'].remove(category)
    else:
        raise ValueError("Category ID {0} not found in service.".format(category_id))

def remove_message_thread(service, thread_id):
    thread = next((t for t in service['MessageThreads'] if t['Thread'] == thread_id), None)
    if thread:
        service['MessageThreads'].remove(thread)
    else:
        raise ValueError("Thread ID {0} not found in service.".format(thread_id))

def remove_message_post(service, thread_id, post_id):
    thread = next((t for t in service['MessageThreads'] if t['Thread'] == thread_id), None)
    if thread is not None:
        message = next((m for m in thread['Messages'] if m['Post'] == post_id), None)
        if message is not None:
            thread['Messages'].remove(message)
        else:
            raise ValueError("Post ID {0} not found in thread {1}.".format(post_id, thread_id))
    else:
        raise ValueError("Thread ID {0} not found in service.".format(thread_id))

def add_service(services, entry, service_name, info=None):
    new_service = {
        'Entry': entry,
        'Service': service_name,
        'Info': info if info else '',
        'Interactions': [],
        'Status': [],
        'Categorization': {'Categories': [], 'Forums': []},
        'Categories': [],
        'Users': {},
        'MessageThreads': []
    }
    services.append(new_service)
    return new_service  # Return the newly created service

def remove_service(services, entry):
    service = next((s for s in services if s['Entry'] == entry), None)
    if service:
        services.remove(service)
    else:
        raise ValueError("Service entry {0} not found.".format(entry))
