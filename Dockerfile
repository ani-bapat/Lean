#
#   LEAN Docker Container 20200522
#   Cross platform deployment for multiple brokerages
#

# Use base system
FROM quantconnect/lean:foundation

MAINTAINER QuantConnect <contact@quantconnect.com>

#Install debugpy and PyDevD for remote python debugging
RUN pip install --no-cache-dir ptvsd==4.3.2 debugpy~=1.6.7 pydevd-pycharm~=231.9225.15

# Install vsdbg for remote C# debugging in Visual Studio and Visual Studio Code
RUN wget https://aka.ms/getvsdbgsh -O - 2>/dev/null | /bin/sh /dev/stdin -v 17.10.20209.7 -l /root/vsdbg

COPY ./DataLibraries /Lean/Launcher/bin/Debug/
COPY ./Lean/Data/ /Lean/Data/
COPY ./Lean/Launcher/bin/Debug/ /Lean/Launcher/bin/Debug/
COPY ./Lean/Optimizer.Launcher/bin/Debug/ /Lean/Optimizer.Launcher/bin/Debug/
COPY ./Lean/Report/bin/Debug/ /Lean/Report/bin/Debug/
COPY ./Lean/DownloaderDataProvider/bin/Debug/ /Lean/DownloaderDataProvider/bin/Debug/

# Install kohinoor
COPY . /app/kohinoor
ENV PYTHONPATH=/app:${PYTHONPATH}

# Can override with '-w'
# WORKDIR /app/kohinoor
# RUN pip install --no-cache-dir -e .
WORKDIR /Lean/Launcher/bin/Debug
# Create initialize script
RUN pip install --no-cache-dir -e /app/kohinoor/

ENTRYPOINT ["dotnet", "QuantConnect.Lean.Launcher.dll"]

# RUN echo "pip install --no-cache-dir -e /app/kohinoor/" > start.sh

# ENTRYPOINT [ "dotnet", "QuantConnect.Lean.Launcher.dll" ]
# RUN chmod +x start.sh
# CMD [ "./start.sh" ]