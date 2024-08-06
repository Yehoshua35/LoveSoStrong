import json
import gzip
import bz2
import lzma
from io import StringIO

def open_compressed_file(filename):
    """ Open a file, trying various compression methods if available. """
    if filename.endswith('.gz'):
        return gzip.open(filename, 'rt', encoding='utf-8')
    elif filename.endswith('.bz2'):
        return bz2.open(filename, 'rt', encoding='utf-8')
    elif filename.endswith('.xz') or filename.endswith('.lzma'):
        return lzma.open(filename, 'rt', encoding='utf-8')
    else:
        return open(filename, 'r', encoding='utf-8')

def save_compressed_file(data, filename):
    """ Save data to a file, using various compression methods if specified. """
    if filename.endswith('.gz'):
        with gzip.open(filename, 'wt', encoding='utf-8') as file:
            file.write(data)
    elif filename.endswith('.bz2'):
        with bz2.open(filename, 'wt', encoding='utf-8') as file:
            file.write(data)
    elif filename.endswith('.xz') or filename.endswith('.lzma'):
        with lzma.open(filename, 'wt', encoding='utf-8') as file:
            file.write(data)
    else:
        with open(filename, 'w', encoding='utf-8') as file:
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
        raise ValueError(f"{variable_name} on line {line_number} should be a non-negative integer, but got '{value}'.")

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
    include_files = []
    user_id = None
    current_bio = None
    current_message = None
    current_thread = None
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

    try:
        for line_number, line in enumerate(lines, 1):
            line = line.strip()
            if line == "--- Include Service Start ---":
                in_include_service = True
                include_files = []
                if verbose:
                    print(f"Line {line_number}: {line} (Starting include service section)")
                continue
            elif line == "--- Include Service End ---":
                in_include_service = False
                if verbose:
                    print(f"Line {line_number}: {line} (Ending include service section)")
                services.extend(parse_include_files(include_files))
                continue
            elif in_include_service:
                include_files.append(line)
                if verbose:
                    print(f"Line {line_number}: {line} (Including file for service)")
                continue
            elif line == "--- Include Users Start ---":
                in_include_users = True
                include_files = []
                if verbose:
                    print(f"Line {line_number}: {line} (Starting include users section)")
                continue
            elif line == "--- Include Users End ---":
                in_include_users = False
                if verbose:
                    print(f"Line {line_number}: {line} (Ending include users section)")
                if current_service:
                    current_service['Users'].update(parse_include_users(include_files))
                continue
            elif in_include_users:
                include_files.append(line)
                if verbose:
                    print(f"Line {line_number}: {line} (Including file for users)")
                continue
            elif line == "--- Include Messages Start ---":
                in_include_messages = True
                include_files = []
                if verbose:
                    print(f"Line {line_number}: {line} (Starting include messages section)")
                continue
            elif line == "--- Include Messages End ---":
                in_include_messages = False
                if verbose:
                    print(f"Line {line_number}: {line} (Ending include messages section)")
                if current_service:
                    current_service['MessageThreads'].extend(parse_include_messages(include_files))
                continue
            elif in_include_messages:
                include_files.append(line)
                if verbose:
                    print(f"Line {line_number}: {line} (Including file for messages)")
                continue
            elif line == "--- Start Archive Service ---":
                current_service = {'Users': {}, 'MessageThreads': [], 'Interactions': []}
                if verbose:
                    print(f"Line {line_number}: {line} (Starting new archive service)")
                continue
            elif line == "--- End Archive Service ---":
                services.append(current_service)
                current_service = None
                if verbose:
                    print(f"Line {line_number}: {line} (Ending archive service)")
                continue
            elif line == "--- Start Comment Section ---":
                in_comment_section = True
                if verbose:
                    print(f"Line {line_number}: {line} (Starting comment section)")
                continue
            elif line == "--- End Comment Section ---":
                in_comment_section = False
                if verbose:
                    print(f"Line {line_number}: {line} (Ending comment section)")
                continue
            elif in_comment_section:
                if verbose:
                    print(f"Line {line_number}: {line} (Comment)")
                continue
            elif current_service is not None:
                key, value = parse_line(line)
                if key == "Entry":
                    current_service['Entry'] = validate_non_negative_integer(value, "Entry", line_number)
                elif key == "Service":
                    current_service['Service'] = value
                elif line == "--- Start User List ---":
                    in_user_list = True
                    if verbose:
                        print(f"Line {line_number}: {line} (Starting user list)")
                    continue
                elif line == "--- End User List ---":
                    in_user_list = False
                    if verbose:
                        print(f"Line {line_number}: {line} (Ending user list)")
                    continue
                elif line == "--- Start User Info ---":
                    in_user_info = True
                    if verbose:
                        print(f"Line {line_number}: {line} (Starting user info)")
                    continue
                elif line == "--- End User Info ---":
                    in_user_info = False
                    user_id = None
                    if verbose:
                        print(f"Line {line_number}: {line} (Ending user info)")
                    continue
                elif line == "--- Start Message List ---":
                    in_message_list = True
                    if verbose:
                        print(f"Line {line_number}: {line} (Starting message list)")
                    continue
                elif line == "--- End Message List ---":
                    in_message_list = False
                    if verbose:
                        print(f"Line {line_number}: {line} (Ending message list)")
                    continue
                elif line == "--- Start Message Thread ---":
                    in_message_thread = True
                    current_thread = {'Title': '', 'Messages': []}
                    post_id = 1
                    if verbose:
                        print(f"Line {line_number}: {line} (Starting message thread)")
                    continue
                elif line == "--- End Message Thread ---":
                    in_message_thread = False
                    current_service['MessageThreads'].append(current_thread)
                    current_thread = None
                    if verbose:
                        print(f"Line {line_number}: {line} (Ending message thread)")
                    continue
                elif line == "--- Start Message Post ---":
                    in_message_post = True
                    current_message = {}
                    if verbose:
                        print(f"Line {line_number}: {line} (Starting message post)")
                    continue
                elif line == "--- End Message Post ---":
                    in_message_post = False
                    if current_message:
                        current_thread['Messages'].append(current_message)
                    current_message = None
                    if verbose:
                        print(f"Line {line_number}: {line} (Ending message post)")
                    continue
                elif in_message_list and key == "Interactions":
                    current_service['Interactions'] = [interaction.strip() for interaction in value.split(",")]
                    if verbose:
                        print(f"Line {line_number}: Interactions set to {current_service['Interactions']}")

                if in_user_list and in_user_info:
                    if key == "User":
                        user_id = validate_non_negative_integer(value, "User", line_number)
                        current_service['Users'][user_id] = {'Bio': ""}
                        if verbose:
                            print(f"Line {line_number}: User ID set to {user_id}")
                    elif key == "Name":
                        if user_id is not None:
                            current_service['Users'][user_id]['Name'] = value
                            if verbose:
                                print(f"Line {line_number}: Name set to {value}")
                    elif key == "Handle":
                        if user_id is not None:
                            current_service['Users'][user_id]['Handle'] = value
                            if verbose:
                                print(f"Line {line_number}: Handle set to {value}")
                    elif key == "Location":
                        if user_id is not None:
                            current_service['Users'][user_id]['Location'] = value
                            if verbose:
                                print(f"Line {line_number}: Location set to {value}")
                    elif key == "Joined":
                        if user_id is not None:
                            current_service['Users'][user_id]['Joined'] = value
                            if verbose:
                                print(f"Line {line_number}: Joined date set to {value}")
                    elif key == "Birthday":
                        if user_id is not None:
                            current_service['Users'][user_id]['Birthday'] = value
                            if verbose:
                                print(f"Line {line_number}: Birthday set to {value}")
                    elif line == "--- Start Bio Body ---":
                        if user_id is not None:
                            current_bio = []
                            in_bio_body = True
                            if verbose:
                                print(f"Line {line_number}: Starting bio body")
                    elif line == "--- End Bio Body ---":
                        if user_id is not None and current_bio is not None:
                            current_service['Users'][user_id]['Bio'] = "\n".join(current_bio)
                            current_bio = None
                            in_bio_body = False
                            if verbose:
                                print(f"Line {line_number}: Ending bio body")
                    elif in_bio_body and current_bio is not None:
                        current_bio.append(line)
                        if verbose:
                            print(f"Line {line_number}: Adding to bio body: {line}")
                elif in_message_list and in_message_thread:
                    if key == "Thread":
                        current_thread['Thread'] = validate_non_negative_integer(value, "Thread", line_number)
                        if verbose:
                            print(f"Line {line_number}: Thread ID set to {value}")
                    elif key == "Title":
                        current_thread['Title'] = value
                        if verbose:
                            print(f"Line {line_number}: Title set to {value}")
                    elif key == "Author":
                        current_message['Author'] = value
                        if verbose:
                            print(f"Line {line_number}: Author set to {value}")
                    elif key == "Time":
                        current_message['Time'] = value
                        if verbose:
                            print(f"Line {line_number}: Time set to {value}")
                    elif key == "Date":
                        current_message['Date'] = value
                        if verbose:
                            print(f"Line {line_number}: Date set to {value}")
                    elif key == "Type":
                        message_type = value
                        if message_type not in current_service['Interactions']:
                            raise ValueError(f"Unexpected message type '{message_type}' found on line {line_number}. Expected one of {current_service['Interactions']}")
                        current_message['Type'] = message_type
                        if verbose:
                            print(f"Line {line_number}: Type set to {message_type}")
                    elif key == "Post":
                        post_value = validate_non_negative_integer(value, "Post", line_number)
                        current_message['Post'] = post_value
                        if 'post_ids' not in current_thread:
                            current_thread['post_ids'] = set()
                        current_thread['post_ids'].add(post_value)
                        if verbose:
                            print(f"Line {line_number}: Post ID set to {post_value}")
                    elif key == "Nested":
                        nested_value = validate_non_negative_integer(value, "Nested", line_number)
                        if nested_value != 0 and nested_value not in current_thread.get('post_ids', set()):
                            raise ValueError(
                                f"Nested value '{nested_value}' on line {line_number} does not match any existing Post values in the current thread. Existing Post IDs: {list(current_thread.get('post_ids', set()))}"
                            )
                        current_message['Nested'] = nested_value
                        if verbose:
                            print(f"Line {line_number}: Nested set to {nested_value}")
                    elif line == "--- Start Message Body ---":
                        if current_message is not None:
                            current_message['Message'] = []
                            in_message_body = True
                            if verbose:
                                print(f"Line {line_number}: Starting message body")
                    elif line == "--- End Message Body ---":
                        if current_message is not None and 'Message' in current_message:
                            current_message['Message'] = "\n".join(current_message['Message'])
                            in_message_body = False
                            if verbose:
                                print(f"Line {line_number}: Ending message body")
                    elif in_message_body and current_message is not None and 'Message' in current_message:
                        current_message['Message'].append(line)
                        if verbose:
                            print(f"Line {line_number}: Adding to message body: {line}")
    except Exception as e:
        if validate_only:
            return False, f"Error: {str(e)}", lines[line_number - 1]
        else:
            raise

    if validate_only:
        return True, "", ""

    return services

def display_services(services):
    for service in services:
        print(f"Service Entry: {service['Entry']}")
        print(f"Service: {service['Service']}")
        print(f"Interactions: {', '.join(service['Interactions'])}")
        print("User List:")
        for user_id, user_info in service['Users'].items():
            print(f"  User ID: {user_id}")
            print(f"    Name: {user_info['Name']}")
            print(f"    Handle: {user_info['Handle']}")
            print(f"    Location: {user_info.get('Location', '')}")
            print(f"    Joined: {user_info.get('Joined', '')}")
            print(f"    Birthday: {user_info.get('Birthday', '')}")
            print(f"    Bio: {user_info.get('Bio', '').strip()}")
            print("")
        print("Message Threads:")
        for idx, thread in enumerate(service['MessageThreads']):
            print(f"  --- Message Thread {idx+1} ---")
            if thread['Title']:
                print(f"    Title: {thread['Title']}")
            for message in thread['Messages']:
                print(f"    {message['Author']} ({message['Time']} on {message['Date']}): [{message['Type']}] Post ID: {message['Post']} Nested: {message['Nested']}")
                print(f"    {message['Message'].strip()}")
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
