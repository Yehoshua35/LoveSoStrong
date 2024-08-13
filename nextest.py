#!/usr/bin/env python

from parse_message_file import *

# Initialize an empty list of services
services = []

# Add a new service with info
new_service = add_service(services, entry=1, service_name="Message Board", info="A simple message board for communicating about anything. ^_^")

# Add users
add_user(new_service, user_id=1, name="Cool Dude 2k", handle="@cooldude2k", location="Somewhere", joined="Jul 1, 2024", birthday="Jul 1, 1987", bio="I'm just a very cool dude! ^_^")
add_user(new_service, user_id=2, name="Kazuki Suzuki", handle="@kazuki.suzuki", location="Anywhere", joined="Jul 1, 2024", birthday="Jun 1, 1987", bio="Hello it's just me Kazuki. :P")

# Add categories
add_category(new_service, kind="Categories", category_type="Category", category_level="Main Category", category_id=1, insub=0, headline="Game Maker 2k", description="Just talk about anything.")
add_category(new_service, kind="Forums", category_type="Forum", category_level="Main Forum", category_id=1, insub=0, headline="General Discussion", description="Just talk about anything.")

# Add message thread
add_message_thread(new_service, thread_id=1, title="Hello, World!", category="Game Maker 2k", forum="General Discussion", thread_type="Topic", state="Pinned")

# Add message posts
add_message_post(new_service, thread_id=1, author="@kazuki.suzuki", time="8:00 AM", date="Jul 1, 2024", subtype="Post", post_id=1, nested=0, message="Hello, World! ^_^")
add_message_post(new_service, thread_id=1, author="@cooldude2k", time="10:00 AM", date="Jul 1, 2024", subtype="Reply", post_id=2, nested=1, message="Why did you say 'Hello, World!' O_o")
add_message_post(new_service, thread_id=1, author="@kazuki.suzuki", time="12:00 PM", date="Jul 1, 2024", subtype="Reply", post_id=3, nested=2, message="I don't know.\nI thought it would be cool. ^_^")
add_message_post(new_service, thread_id=1, author="@cooldude2k", time="2:00 PM", date="Jul 1, 2024", subtype="Reply", post_id=4, nested=3, message="What ever dude! <_<")

# Add another service
another_service = add_service(services, entry=2, service_name="Another Board", info="Another simple message board.")

# Display the services
display_services(services)

# Remove a service
remove_service(services, entry=1)

# Display the services again
display_services(services)
