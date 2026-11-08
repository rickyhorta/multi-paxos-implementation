package didameetings.util;

import java.util.ArrayList;

public abstract class GenericResponseProcessor<T>  {

    public abstract boolean onNext(ArrayList<T> all_responses, T last_response);

    public abstract boolean onCompleted(ArrayList<T> all_responses);
}
