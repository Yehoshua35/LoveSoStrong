from __future__ import absolute_import, division, print_function, unicode_literals
import json
import gzip
import bz2
import sys
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
    else:
        with io.open(filename, 'w', encoding='utf-8') as file:
            file.write(data)

def parse_line(line):
    """ Parse a line in the format 'var: value' and return the key and value. """
    parts = line.split(":", 1)
    if len(parts) == 2:
        key = parts[0].strip()
        value = parts[1].strip()
        return key, value
    return None, None

def validate_non_negative_integer(value, variable_name, line_number):
    """ Validate and convert a value to a non-negative integer. """
    try:
        int_value = int(value)
        if int_value < 0:
            raise ValueError
        return int_value
    except ValueError:
        raise ValueError("{0} on line {1} should be a non-negative integer, but got '{2}'.".format(variable_name, line_number, value))

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
    in_user_list = False
    in_message_list = False
    in_message_thread = False
    in_user_info = False
    in_message_post = False
    in_bio_body = False
    in_message_body = False
    in_comment_section = False
    in_include_service = False
    in_include_users = False
    in_include_messages = False
    in_category_list = False
    in_description_body = False
    in_include_categories = False
    in_categorization_list = False
    include_files = []
    user_id = None
    current_bio = None
    current_message = None
    current_thread = None
    current_category = None
    categorization_values = []
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
                in_include_service = True
                include_files = []
                if verbose:
                    print("Line {0}: {1} (Starting include service section)".format(line_number, line))
                continue
            elif line == "--- Include Service End ---":
                in_include_service = False
                if verbose:
                    print("Line {0}: {1} (Ending include service section)".format(line_number, line))
                services.extend(parse_include_files(include_files))
                continue
            elif in_include_service:
                include_files.append(line)
                if verbose:
                    print("Line {0}: {1} (Including file for service)".format(line_number, line))
                continue
            elif line == "--- Include Users Start ---":
                in_include_users = True
                include_files = []
                if verbose:
                    print("Line {0}: {1} (Starting include users section)".format(line_number, line))
                continue
            elif line == "--- Include Users End ---":
                in_include_users = False
                if verbose:
                    print("Line {0}: {1} (Ending include users section)".format(line_number, line))
                if current_service:
                    current_service['Users'].update(parse_include_users(include_files))
                continue
            elif in_include_users:
                include_files.append(line)
                if verbose:
                    print("Line {0}: {1} (Including file for users)".format(line_number, line))
                continue
            elif line == "--- Include Messages Start ---":
                in_include_messages = True
                include_files = []
                if verbose:
                    print("Line {0}: {1} (Starting include messages section)".format(line_number, line))
                continue
            elif line == "--- Include Messages End ---":
                in_include_messages = False
                if verbose:
                    print("Line {0}: {1} (Ending include messages section)".format(line_number, line))
                if current_service:
                    current_service['MessageThreads'].extend(parse_include_messages(include_files))
                continue
            elif in_include_messages:
                include_files.append(line)
                if verbose:
                    print("Line {0}: {1} (Including file for messages)".format(line_number, line))
                continue
            elif line == "--- Include Categories Start ---":
                in_include_categories = True
                include_files = []
                if verbose:
                    print("Line {0}: {1} (Starting include categories section)".format(line_number, line))
                continue
            elif line == "--- Include Categories End ---":
                in_include_categories = False
                if verbose:
                    print("Line {0}: {1} (Ending include categories section)".format(line_number, line))
                if current_service:
                    current_service['Categories'].extend(parse_include_categories(include_files))
                    for category in current_service['Categories']:
                        category_ids[category['Type']].add(category['ID'])
                continue
            elif in_include_categories:
                include_files.append(line)
                if verbose:
                    print("Line {0}: {1} (Including file for categories)".format(line_number, line))
                continue
            elif line == "--- Start Archive Service ---":
                current_service = {'Users': {}, 'MessageThreads': [], 'Categories': [], 'Interactions': [], 'Categorization': []}
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
                in_comment_section = True
                if verbose:
                    print("Line {0}: {1} (Starting comment section)".format(line_number, line))
                continue
            elif line == "--- End Comment Section ---":
                in_comment_section = False
                if verbose:
                    print("Line {0}: {1} (Ending comment section)".format(line_number, line))
                continue
            elif in_comment_section:
                if verbose:
                    print("Line {0}: {1} (Comment)".format(line_number, line))
                continue
            elif line == "--- Start Category List ---":
                in_category_list = True
                current_category = {}
                if verbose:
                    print("Line {0}: {1} (Starting category list)".format(line_number, line))
                continue
            elif line == "--- End Category List ---":
                in_category_list = False
                if current_category:
                    kind_split = current_category.get('Kind', '').split(",")
                    current_category['Type'] = kind_split[0].strip()
                    current_category['Level'] = kind_split[1].strip()
                    if current_category['Type'] not in categorization_values:
                        raise ValueError("Invalid 'Type' value '{0}' on line {1}. Expected one of {2}.".format(current_category['Type'], line_number, categorization_values))
                    if current_category['InSub'] != 0 and current_category['InSub'] not in category_ids[current_category['Type']]:
                        raise ValueError("InSub value '{0}' on line {1} does not match any existing ID values.".format(current_category['InSub'], line_number))
                    current_service['Categories'].append(current_category)
                    category_ids[current_category['Type']].add(current_category['ID'])
                current_category = None
                if verbose:
                    print("Line {0}: {1} (Ending category list)".format(line_number, line))
                continue
            elif line == "--- Start Categorization List ---":
                in_categorization_list = True
                current_service['Categorization'] = {}
                if verbose:
                    print("Line {0}: {1} (Starting categorization list)".format(line_number, line))
                continue
            elif line == "--- End Categorization List ---":
                in_categorization_list = False
                if verbose:
                    print("Line {0}: {1} (Ending categorization list)".format(line_number, line))
                categorization_values = set(current_service['Categorization'].keys())
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
                elif in_category_list:
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
                    in_user_list = True
                    if verbose:
                        print("Line {0}: {1} (Starting user list)".format(line_number, line))
                    continue
                elif line == "--- End User List ---":
                    in_user_list = False
                    if verbose:
                        print("Line {0}: {1} (Ending user list)".format(line_number, line))
                    continue
                elif line == "--- Start User Info ---":
                    in_user_info = True
                    if verbose:
                        print("Line {0}: {1} (Starting user info)".format(line_number, line))
                    continue
                elif line == "--- End User Info ---":
                    in_user_info = False
                    user_id = None
                    if verbose:
                        print("Line {0}: {1} (Ending user info)".format(line_number, line))
                    continue
                elif line == "--- Start Message List ---":
                    in_message_list = True
                    if verbose:
                        print("Line {0}: {1} (Starting message list)".format(line_number, line))
                    continue
                elif line == "--- End Message List ---":
                    in_message_list = False
                    if verbose:
                        print("Line {0}: {1} (Ending message list)".format(line_number, line))
                    continue
                elif line == "--- Start Message Thread ---":
                    in_message_thread = True
                    current_thread = {'Title': '', 'Messages': []}
                    post_id = 1
                    if verbose:
                        print("Line {0}: {1} (Starting message thread)".format(line_number, line))
                    continue
                elif line == "--- End Message Thread ---":
                    in_message_thread = False
                    current_service['MessageThreads'].append(current_thread)
                    current_thread = None
                    if verbose:
                        print("Line {0}: {1} (Ending message thread)".format(line_number, line))
                    continue
                elif line == "--- Start Message Post ---":
                    in_message_post = True
                    current_message = {}
                    if verbose:
                        print("Line {0}: {1} (Starting message post)".format(line_number, line))
                    continue
                elif line == "--- End Message Post ---":
                    in_message_post = False
                    if current_message:
                        current_thread['Messages'].append(current_message)
                    current_message = None
                    if verbose:
                        print("Line {0}: {1} (Ending message post)".format(line_number, line))
                    continue
                elif in_message_list and key == "Interactions":
                    current_service['Interactions'] = [interaction.strip() for interaction in value.split(",")]
                    if verbose:
                        print("Line {0}: Interactions set to {1}".format(line_number, current_service['Interactions']))

                if in_user_list and in_user_info:
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
                            in_bio_body = True
                            if verbose:
                                print("Line {0}: Starting bio body".format(line_number))
                    elif line == "--- End Bio Body ---":
                        if user_id is not None and current_bio is not None:
                            current_service['Users'][user_id]['Bio'] = "\n".join(current_bio)
                            current_bio = None
                            in_bio_body = False
                            if verbose:
                                print("Line {0}: Ending bio body".format(line_number))
                    elif in_bio_body and current_bio is not None:
                        current_bio.append(line)
                        if verbose:
                            print("Line {0}: Adding to bio body: {1}".format(line_number, line))
                elif in_message_list and in_message_thread:
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
                    elif key == "Type":
                        message_type = value
                        if message_type not in current_service['Interactions']:
                            raise ValueError("Unexpected message type '{0}' found on line {1}. Expected one of {2}".format(message_type, line_number, current_service['Interactions']))
                        current_message['Type'] = message_type
                        if verbose:
                            print("Line {0}: Type set to {1}".format(line_number, message_type))
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
                            in_message_body = True
                            if verbose:
                                print("Line {0}: Starting message body".format(line_number))
                    elif line == "--- End Message Body ---":
                        if current_message is not None and 'Message' in current_message:
                            current_message['Message'] = "\n".join(current_message['Message'])
                            in_message_body = False
                            if verbose:
                                print("Line {0}: Ending message body".format(line_number))
                    elif in_message_body and current_message is not None and 'Message' in current_message:
                        current_message['Message'].append(line)
                        if verbose:
                            print("Line {0}: Adding to message body: {1}".format(line_number, line))
    except Exception as e:
        if validate_only:
            return False, "Error: {0}".format(str(e)), lines[line_number - 1]
        else:
            raise

    if validate_only:
        return True, "", ""

    return services

def display_services(services):
    for service in services:
        print("Service Entry: {0}".format(service['Entry']))
        print("Service: {0}".format(service['Service']))
        print("Interactions: {0}".format(', '.join(service['Interactions'])))
        if 'Categorization' in service and service['Categorization']:
            for category_type, category_levels in service['Categorization'].items():
                print("{0}: {0}".format(category_type, ', '.join(category_levels)))
        print("Category List:")
        for category in service['Categories']:
            print("  Type: {0}, Level: {1}".format(category['Type'], category['Level']))
            print("  ID: {0}".format(category['ID']))
            print("  InSub: {0}".format(category['InSub']))
            print("  Headline: {0}".format(category['Headline']))
            print("  Description: {0}".format(category['Description'].strip()))
            print("")
        print("User List:")
        for user_id, user_info in service['Users'].items():
            print("  User ID: {0}".format(user_id))
            print("    Name: {0}".format(user_info['Name']))
            print("    Handle: {0}".format(user_info['Handle']))
            print("    Location: {0}".format(user_info.get('Location', '')))
            print("    Joined: {0}".format(user_info.get('Joined', '')))
            print("    Birthday: {0}".format(user_info.get('Birthday', '')))
            print("    Bio: {0}".format(user_info.get('Bio', '').strip()))
            print("")
        print("Message Threads:")
        for idx, thread in enumerate(service['MessageThreads']):
            print("  --- Message Thread {0} ---".format(idx+1))
            if thread['Title']:
                print("    Title: {0}".format(thread['Title']))
            if 'Category' in thread:
                print("    Category: {0}".format(', '.join(thread['Category'])))
            if 'Forum' in thread:
                print("    Forum: {0}".format(', '.join(thread['Forum'])))
            for message in thread['Messages']:
                print("    {0} ({1} on {2}): [{3}] Post ID: {4} Nested: {5}".format(
                    message['Author'], message['Time'], message['Date'], message['Type'], message['Post'], message['Nested']))
                print("    {0}".format(message['Message'].strip()))
            print("")

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

def save_to_json_file(services, json_filename):
    """ Save the services data structure to a JSON file """
    json_data = json.dumps(services, indent=2)
    save_compressed_file(json_data, json_filename)

def services_to_string(services, line_ending="lf"):
    """ Convert the services data structure back to the original text format """
    lines = []
    for service in services:
        lines.append("--- Start Archive Service ---")
        lines.append("Entry: {0}".format(service['Entry']))
        lines.append("Service: {0}".format(service['Service']))
        
        lines.append("--- Start User List ---")
        for user_id, user_info in service['Users'].items():
            lines.append("--- Start User Info ---")
            lines.append("User: {0}".format(user_id))
            lines.append("Name: {0}".format(user_info['Name']))
            lines.append("Handle: {0}".format(user_info['Handle']))
            if 'Location' in user_info:
                lines.append("Location: {0}".format(user_info['Location']))
            if 'Joined' in user_info:
                lines.append("Joined: {0}".format(user_info['Joined']))
            if 'Birthday' in user_info:
                lines.append("Birthday: {0}".format(user_info['Birthday']))
            if 'Bio' in user_info:
                lines.append("Bio:")
                lines.append("--- Start Bio Body ---")
                lines.extend(user_info['Bio'].split("\n"))
                lines.append("--- End Bio Body ---")
            lines.append("--- End User Info ---")
        lines.append("--- End User List ---")
        
        if 'Categorization' in service and service['Categorization']:
            lines.append("--- Start Categorization List ---")
            for category_type, category_levels in service['Categorization'].items():
                lines.append("{0}: {1}".format(category_type, ', '.join(category_levels)))
            lines.append("--- End Categorization List ---")
        
        if 'Categories' in service and service['Categories']:
            for category in service['Categories']:
                lines.append("--- Start Category List ---")
                lines.append("Kind: {0}, {1}".format(category['Type'], category['Level']))
                lines.append("ID: {0}".format(category['ID']))
                lines.append("InSub: {0}".format(category['InSub']))
                lines.append("Headline: {0}".format(category['Headline']))
                lines.append("Description: {0}".format(category['Description']))
                lines.append("--- End Category List ---")
        
        lines.append("--- Start Message List ---")
        lines.append("Interactions: {0}".format(', '.join(service['Interactions'])))
        for thread in service['MessageThreads']:
            lines.append("--- Start Message Thread ---")
            lines.append("Thread: {0}".format(thread['Thread']))
            if 'Category' in thread:
                lines.append("Category: {0}".format(', '.join(thread['Category'])))
            if 'Forum' in thread:
                lines.append("Forum: {0}".format(', '.join(thread['Forum'])))
            if 'Title' in thread:
                lines.append("Title: {0}".format(thread['Title']))
            for message in thread['Messages']:
                lines.append("--- Start Message Post ---")
                lines.append("Author: {0}".format(message['Author']))
                lines.append("Time: {0}".format(message['Time']))
                lines.append("Date: {0}".format(message['Date']))
                lines.append("Type: {0}".format(message['Type']))
                lines.append("Post: {0}".format(message['Post']))
                lines.append("Nested: {0}".format(message['Nested']))
                lines.append("Message:")
                lines.append("--- Start Message Body ---")
                lines.extend(message['Message'].split("\n"))
                lines.append("--- End Message Body ---")
                lines.append("--- End Message Post ---")
            lines.append("--- End Message Thread ---")
        lines.append("--- End Message List ---")
        
        lines.append("--- End Archive Service ---")
    
    line_sep = {"lf": "\n", "cr": "\r", "crlf": "\r\n"}
    return line_sep.get(line_ending, "\n").join(lines)

def save_services_to_file(services, filename, line_ending="lf"):
    """ Save the services data structure to a file in the original text format """
    data = services_to_string(services, line_ending)
    save_compressed_file(data, filename)
