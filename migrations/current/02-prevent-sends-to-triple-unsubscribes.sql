--Table for storing which phone has unsubscribed from which profile
--Automatically indexed by phone_number, no need to explicitly declare an index to get better performance
create table unsubscribed_phone_numbers (
    phone_number text unique primary key,
    unsubscribed_profiles uuid[] default '{}'
);
--if the table delivery_reports had already been committed and had data, we would need a step to populate unsubscribed_phone_numbers but I'm leaving that out for this assignment since delivery_reports is in current 

--Trigger for adding to unsubscribed
create or replace function unsubscribe_by_phone() 
returns trigger
as $$
    declare
        new_phone_number outbound_messages.to_number%type;
        new_profile_id   outbound_messages.profile_id%type;
        profiles_array unsubscribed_phone_numbers.unsubscribed_profiles%type;
        loop_id uuid;
    begin
        select a.to_number, a.profile_id, b.unsubscribed_profiles
            into new_phone_number, new_profile_id, profiles_array
            from outbound_messages as a
            left join unsubscribed_phone_numbers as b
            on b.phone_number = a.to_number
            where id = NEW.message_id;
        if profiles_array is null then
            --if this phone has never unsubscribed
            insert into unsubscribed_phone_numbers 
            values (new_phone_number,array[new_profile_id]);
            return NEW;
        end if;
        foreach loop_id in Array profiles_array
        loop
            if loop_id = new_profile_id --profiles in the array should be unique 
            then 
                return NEW;
            end if;
        end loop;
        update unsubscribed_phone_numbers 
            set unsubscribed_profiles = array_append(unsubscribed_profiles,new_profile_id)
            where phone_number = new_phone_number;
        return NEW;
    end
$$
language plpgsql;

create trigger unsubscribed_by_phone
    after insert 
    on delivery_reports
    for each row
    when (NEW.error_code = 21610 )
    execute procedure unsubscribe_by_phone();

--Trigger for checking unsubscribed_phone_numbers and throwing an error
create or replace function check_unsubscribed_phone_numbers() 
returns trigger 
as $$
declare profiles_array  unsubscribed_phone_numbers.unsubscribed_profiles%type;
begin
    select unsubscribed_profiles
        into profiles_array
        from  unsubscribed_phone_numbers 
        where phone_number = NEW.to_number;
        
   if array_length(profiles_array,1) >= 3 then
     raise 'Cannot send message - frequently unsubscribed recipient';
   end if;
   return NEW;
end;
$$ language plpgsql;

create trigger check_unsubscribed_phone_numbers
  before insert
  on outbound_messages
  for each row
  execute procedure check_unsubscribed_phone_numbers();
